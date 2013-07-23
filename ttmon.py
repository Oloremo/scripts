#!/usr/bin/env python

import socket              # for network
import subprocess          # for "ps aux"
import re                  # for regexps
from glob import glob      # for fs file paths
from sys import exit       # for exit code
from os import chdir       # for glob()
from select import select  # for socket reading
from optparse import OptionParser, OptionGroup  # for options parser
from os.path import isfile  # for OS file check
import errno                # for exeption handling

### Gotta catch 'em all!
usage = "usage: %prog -t TYPE [-c LIMIT] [-w LIMIT] [-i LIMIT] [--exit NUM]"

parser = OptionParser(usage=usage)
parser.add_option('-t', '--type', type='choice', action='store', dest='type',
                 choices=['slab', 'repl', 'infr_cvp', 'infr_pvc', 'infr_ivc'],
                 help='Check type. Chose from "slab", "repl", "infr_cvp", "infr_pvc", "infr_ivc"')

group = OptionGroup(parser, "Ajusting limits")
group.add_option("-c", dest="crit_limit", type="int", help="Critical limit. Defaults: slab = 90. repl = 10")
group.add_option("-w", dest="warn_limit", type="int", help="Warning limit. Defaults slab = 80. repl = 5")
group.add_option("-i", dest="info_limit", type="int", help="Info limit. Defaults slab = 70. repl = 1")
group.add_option("--exit", dest="exit_code", type="int", default="3", help="Exit code for infrastructure monitoring. Default: 3(Info)")
parser.add_option_group(group)

(opts, args) = parser.parse_args()

if opts.type == 'slab':
        if not opts.crit_limit:
                opts.crit_limit = 90
        if not opts.warn_limit:
                opts.warn_limit = 80
        if not opts.info_limit:
                opts.info_limit = 70
elif opts.type == 'repl':
        if not opts.crit_limit:
                opts.crit_limit = 10
        if not opts.warn_limit:
                opts.warn_limit = 5
        if not opts.info_limit:
                opts.info_limit = 1

### Global vars
cfg_paths_list = ['/usr/local/etc/tarantool*.cfg', '/usr/local/etc/octopus*.cfg']
init_paths_list = ['/etc/init.d/tarantool*', '/etc/init.d/octopus*']
proc_pattern = '.*(tarantool|octopus).* adm:.*\d+.*'
sock_timeout = 0.5

### Functions
def open_file(filename):
    """ We try to open file and copy it into list. """

    if not isfile(filename):
        print "I/O error. There is no '%s' but we find it before by glob. Check me." % filename
        raise Exception('NO_FILE')
    try:
        return list(open(filename))
    except IOError, err:
        print "I/O error. Can't open file '%s'. Check me." % filename
        print "Error %s: %s" % (err.errno, err.strerror)
        raise Exception('IO_ERROR')
    except:
        raise Exception

def open_socket(sock, timeout, host, port):
    """ We try to open socket here and catch nasty exeptions if we can't """

    try:
        sock.settimeout(timeout)
        sock.connect((host, int(port)))
        return True
    except socket.timeout, err:
        raise Exception('TO_ERROR')
    except socket.error, err:
        if hasattr(err, 'errno'):
            if err.errno == errno.ECONNREFUSED:
                raise Exception('ECONNREFUSED')
        else:
            if err[1] == "Connection refused":
                raise Exception('ECONNREFUSED')

        ### If none of above - we have a unhandled exeption.
        print "Socket error. Unknown. Port was %s" % port
        print err
        raise Exception
        exit(1)
    return False

def read_socket(sock, timeout=1, recv_buffer=4096):
    """ Nice way to read from socket. We use select() for timeout and recv handling """

    buffer = ''
    receiving = True
    while receiving:
            ready = select([sock], [], [], timeout)
            if ready[0]:
                    data = sock.recv(recv_buffer)
                    buffer += data

                    ### Have we reached end of data?
                    for line in buffer.splitlines():
                            if '---' in line:
                                    receiving = False
            else:
                    buffer = 'check_error: Timeout after %s second' % timeout
                    receiving = False

    for line in buffer.splitlines():
            yield line

def get_stats(sock, commands, arg, timeout=1, recv_buffer=4096):
    """ Parsing internal tt\octopus info from admin port """

    args_dict = {}
    for my_arg in arg:
        args_dict[my_arg] = ''
    args_dict['recovery_lag'] = 0

    for command in commands:
        sock.sendall(command)

        for line in read_socket(sock, timeout):
            for my_arg in arg:
                if my_arg in line:
                    args_dict[my_arg] = line.split(':', -1)[1]

    sock.sendall('quit\n')
    return args_dict

def make_cfg_dict(cfg_list):
    """ Making dict from tt\octopus cfg's """

    cfg_dict_loc = {}
    for cfg_file in cfg_list:
            try:
                file_list = open_file(cfg_file)
            except Exception, err:
                if 'NO_FILE' in err:
                    exit(2)
                elif 'IO_ERROR' in err:
                    exit(1)
                else:
                    print "Fatal error. Something bad happend. Check me."
                    print err
                    exit(1)

            cfg_dict_loc[cfg_file] = {'pport': '', 'aport': ''}
            for string in file_list:
                if 'primary_port' in string:
                    cfg_dict_loc[cfg_file]['pport'] = string.split()[2]
                elif 'admin_port' in string:
                    cfg_dict_loc[cfg_file]['aport'] = string.split()[2]

    return cfg_dict_loc

def make_proc_dict(adm_port_list, host='localhost'):
    """ Making dict from running tt\octopus """

    adm_dict_loc = {}
    for aport in adm_port_list:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            open_socket(sock, sock_timeout, host, aport)
        except Exception, err:
            if 'TO_ERROR' in err:
                adm_dict_loc[aport] = {'aport': aport, 'check_error': "Timeout after %s second" % sock_timeout}
            elif 'ECONNREFUSED' in err:
                adm_dict_loc[aport] = {'aport': aport, 'check_error': "Connection refused"}
            else:
                print err
                exit(1)

        args_dict = get_stats(sock, ['show slab\n', 'show info\n'], ['items_used', 'arena_used', 'recovery_lag', 'config', 'check_error'], sock_timeout)
        args_dict['aport'] = aport
        sock.close()

        filters = {
            'items_used': lambda x: int(x.rsplit('.')[0]),
            'arena_used': lambda x: int(x.rsplit('.')[0]),
            'recovery_lag': lambda x: int(x.rsplit('.')[0]),
            'config': lambda x: int(x.strip(' "')),
        }

        for key in set(args_dict.keys()) & set(filters.keys()):
            if args_dict[key] != '' and args_dict[key] != 0:
                args_dict[key] = filters[key](args_dict[key])

        adm_dict_loc[aport] = args_dict

    return adm_dict_loc

def make_paths_list(paths, basename=False):
    """ Make a list with paths to files. Full path to cfg and just basename for init scripts """

    paths_list_loc = []
    for path in paths:
            if basename:
                    path = path.rsplit('/', 1)
                    chdir(path[0])
                    if len(glob(path[1])) != 0:
                            paths_list_loc.extend(glob(path[1]))
            else:
                    if len(glob(path)) != 0:
                            paths_list_loc.extend(glob(path))

    return paths_list_loc

def make_tt_proc_list(pattern):
    """ Making list of a running tt\octopus process to parse after """

    ps = subprocess.Popen(['ps', '-eo' 'args'], stdout=subprocess.PIPE).communicate()[0]
    tt_proc_list_loc = []
    p = re.compile(pattern)
    for line in ps.splitlines():
            if p.match(line):
                    tt_proc_list_loc.append(line)

    return tt_proc_list_loc

def make_chkcfg_list():
    """ Making a list of init scripts added to chkconfig """

    chkcfg = subprocess.Popen(['chkconfig', '--list'], stdout=subprocess.PIPE).communicate()[0]
    chkcfg_list_loc = []
    for line in chkcfg.splitlines():
            if '3:on' in line:
                    chkcfg_list_loc.append(line)

    return chkcfg_list_loc

def make_adm_port_list(tt_proc_list):
    """ Parsing tt_proc list to get admin ports from it """

    p = re.compile(' adm:.*\d+')
    d = re.compile('\d+')
    adm_port_list_loc = []
    for tt_proc in tt_proc_list:
            if p.findall(tt_proc):
                    aport = p.findall(tt_proc)[0]
                    aport = d.findall(aport)[0]
                    adm_port_list_loc.append(aport)

    return adm_port_list_loc

def print_alert(check_item, size, limit, aport, error):
    """ Helper fuction to print nice alrts """

    if error != '':
        return 'Octopus/Tarantool with admin port %s runs on error: %s' % (aport, error)
    else:
        return 'Octopus/Tarantool with admin port %s. "%s" is more than %s - %s' % (aport, check_item, limit, size)

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """

    for string in list:
        print string

def check_cfg_vs_proc(cfg_dict):
    """ Check configs vs proccesses """

    for cfg in cfg_dict.keys():
            p = re.compile('.*(tarantool|octopus).* adm:.*%s.*' % cfg_dict[cfg]['aport'])
            if not filter(p.match, tt_proc_list):
                    yield "Octopus/Tarantool with config %s is not running!" % cfg

def check_proc_vs_cfg(proc_dict, cfg_dict):
    """ Check proccess vs configs """

    for proc in proc_dict.itervalues():
            if not proc['config'] in cfg_dict.keys():
                    yield "Octopus/Tarantool with admin port %s is running without config!" % proc['aport']

def check_init_vs_chk(init_list, chkcfg_list):
    """ Check init scripts vs chkconfig """

    if len(init_list) == 0:
        yield "Octopus/Tarantool init scripts not found!"
    else:
        for init in init_list:
            p = re.compile(r'^%s.*3:on.*' % init)
            if not filter(p.search, chkcfg_list):
                yield 'Init script "%s" is not added to chkconfig!' % init

def check_infrastructure(exit_code, infr_cvp=False, infr_pvc=False, infr_ivc=False):
    """ Main infrastructure check """

    errors_list = []

    if infr_cvp:
        for alert in check_cfg_vs_proc(cfg_dict):
                errors_list.append(alert)

    if infr_pvc:
        for alert in check_proc_vs_cfg(proc_dict, cfg_dict):
                errors_list.append(alert)

    if infr_ivc:
        for alert in check_init_vs_chk(init_list, chkcfg_list):
                errors_list.append(alert)

    if len(errors_list) != 0:
            print_list(errors_list)
            exit(exit_code)

def check_stats(adm_port_list, proc_dict, crit, warn, info, check_repl=False):
    """ Check stats from proccess against limits """

    result_critical = []
    result_warning = []
    result_info = []

    for proc in proc_dict.keys():
        aport = proc_dict[proc]['aport']
        error = proc_dict[proc]['check_error']
        if error != '':
            result_critical.append(print_alert('', '', '', aport, error))
            continue
        items_used = proc_dict[proc]['items_used']
        arena_used = proc_dict[proc]['arena_used']
        rep_lag = proc_dict[proc]['recovery_lag']

        if check_repl:

                if rep_lag == '':
                    result_critical.append(print_alert('', '', '', aport, "Can't get replication lag info. Check me."))
                    continue

                if rep_lag >= crit:
                        result_critical.append(print_alert('replication_lag', rep_lag, crit, aport, error))
                elif rep_lag >= warn:
                        result_warning.append(print_alert('replication_lag', rep_lag, warn, aport, error))
                elif rep_lag >= info:
                        result_info.append(print_alert('replication_lag', rep_lag, info, aport, error))
        else:
                if items_used >= crit:
                        result_critical.append(print_alert('items_used', items_used, crit, aport, error))
                elif items_used >= warn:
                        result_warning.append(print_alert('items_used', items_used, warn, aport, error))
                elif items_used >= info:
                        result_info.append(print_alert('items_used', items_used, info, aport, error))

                if arena_used >= crit:
                        result_critical.append(print_alert('arena_used', arena_used, crit, aport, error))
                elif arena_used >= warn:
                        result_warning.append(print_alert('arena_used', arena_used, warn, aport, error))
                elif arena_used >= info:
                        result_info.append(print_alert('arena_used', arena_used, info, aport, error))

    ### Depending on situation it prints revelant list filled with alert strings
    if len(result_critical) != 0 and len(result_warning) != 0:
        print_list(result_critical)
        print_list(result_warning)
        exit(1)
    elif len(result_critical) != 0 and len(result_warning) == 0:
        print_list(result_critical)
        exit(1)
    elif len(result_warning) != 0:
        print_list(result_warning)
        exit(2)
    elif len(result_info) != 0:
        print_list(result_info)
        exit(3)

### Do the work
if opts.type == 'infr_cvp':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_adm_port_list(tt_proc_list)
    cfg_list = make_paths_list(cfg_paths_list)
    cfg_dict = make_cfg_dict(cfg_list)

    ### Check stuff
    check_infrastructure(opts.exit_code, infr_cvp=True)

if opts.type == 'infr_pvc':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_adm_port_list(tt_proc_list)
    cfg_list = make_paths_list(cfg_paths_list)
    cfg_dict = make_cfg_dict(cfg_list)
    proc_dict = make_proc_dict(adm_port_list)

    ### Check stuff
    check_infrastructure(opts.exit_code, infr_pvc=True)

if opts.type == 'infr_ivc':
    ### Make stuff
    init_list = make_paths_list(init_paths_list, basename=True)
    chkcfg_list = make_chkcfg_list()

    ### Check stuff
    check_infrastructure(opts.exit_code, infr_ivc=True)

if opts.type == 'slab':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_adm_port_list(tt_proc_list)
    proc_dict = make_proc_dict(adm_port_list)

    ### Check stuff
    check_stats(adm_port_list, proc_dict, opts.crit_limit, opts.warn_limit, opts.info_limit)

if opts.type == 'repl':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_adm_port_list(tt_proc_list)
    proc_dict = make_proc_dict(adm_port_list)

    ### Check stuff
    check_stats(adm_port_list, proc_dict, opts.crit_limit, opts.warn_limit, opts.info_limit, check_repl=True)
