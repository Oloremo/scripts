#!/usr/bin/env python

import socket              # for network
import subprocess          # for "ps aux"
import re                  # for regexps
from glob import glob      # for fs file paths
from sys import exit, stdout, version_info      # for exit code, output func and version check
from os import chdir       # for glob()
from select import select  # for socket reading
from optparse import OptionParser, OptionGroup  # for options parser
from os.path import isfile  # for OS file check
import errno                # for exeption handling
import MySQLdb              # for mysql

### Gotta catch 'em all!
usage = "usage: %prog -t TYPE [-c LIMIT] [-w LIMIT] [-i LIMIT] [--exit NUM]"

parser = OptionParser(usage=usage)
parser.add_option('-t', '--type', type='choice', action='store', dest='type',
                 choices=['slab', 'repl', 'infr_cvp', 'infr_pvc', 'infr_ivc', 'pinger', 'octopus_crc'],
                 help='Check type. Chose from "slab", "repl", "infr_cvp", "infr_pvc", "infr_ivc", "pinger", "octopus_crc"')

group = OptionGroup(parser, "Ajusting limits")
group.add_option("-c", dest="crit_limit", type="int", help="Critical limit. Defaults: slab = 90. repl = 10")
group.add_option("-w", dest="warn_limit", type="int", help="Warning limit. Defaults slab = 80. repl = 5")
group.add_option("-i", dest="info_limit", type="int", help="Info limit. Defaults slab = 70. repl = 1")
group.add_option("--exit", dest="exit_code", type="int", default="3", help="Exit code for infrastructure monitoring. Default: 3(Info)")
group.add_option("--conf", dest="config", type="str", default="/etc/ttmon.conf", help="Config file. Used in pinger check. Default: /etc/ttmon.conf")
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
cfg_paths_list = ['/usr/local/etc/tarantool*.cfg', '/usr/local/etc/octopus*.cfg', '/etc/tarantool/*.cfg']
cfg_excl_re = 'tarantool.*feeder.*.cfg$'
init_paths_list = ['/etc/init.d/tarantool*', '/etc/init.d/octopus*']
proc_pattern = '.*(tarantool|octopus).* adm:.*\d+.*'
octopus_repl_pattern = '.*(octopus: box:hot_standby).* adm:.*\d+.*'
sock_timeout = 0.1
crc_lag_limit = 2220
general_dict = {'show slab': ['items_used', 'arena_used'],
               'show info': ['recovery_lag', 'config'],
               'show configuration': ['primary_port']}
crc_check_dict = {'show configuration': ['wal_feeder_addr'],
                  'show info': ['recovery_run_crc_lag', 'recovery_run_crc_status']}

if version_info[1] >= 6:
    ### Python 2.6
    isEL6 = True
else:
    ### Python 2.4
    isEL6 = False

### Functions

def output(line):
    if isEL6:
        stdout.write(line + "<br>")
        stdout.flush()
    else:
        print line

def open_file(filename):
    """ We try to open file and copy it into list. """

    if not isfile(filename):
        output("I/O error. There is no '%s'. Check me." % filename)
        raise Exception('NO_FILE')
    try:
        return list(open(filename))
    except IOError, err:
        output("I/O error. Can't open file '%s'. Check me." % filename)
        output("Error %s: %s" % (err.errno, err.strerror))
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
            elif err.errno == errno.EPIPE:
                raise Exception('EPIPE')
        else:
            if err[1] == "Connection refused":
                raise Exception('ECONNREFUSED')
            elif err[1] == "Broken pipe":
                raise Exception('EPIPE')

        ### If none of above - we have a unhandled exeption.
        output("Socket error. Unknown. Port was %s" % port)
        output(err)
        raise Exception
        exit(1)
    return False

def read_socket(sock, timeout=1, recv_buffer=262144):
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
                            if '...' in line:
                                receiving = False
            else:
                    buffer = 'check_error: Timeout after %s second' % timeout
                    receiving = False

    for line in buffer.splitlines():
        yield line

def get_stats(sock, lookup_dict, timeout=0.1, recv_buffer=262144):
    """ Parsing internal tt\octopus info from admin port """

    args_dict = {}
    for list in lookup_dict.itervalues():
        for arg in list:
                args_dict[arg] = ''
    args_dict['recovery_lag'] = 0
    args_dict['check_error'] = ''

    for command in lookup_dict.keys():
        try:
            sock.sendall(command + '\n')
            args_set = set(lookup_dict[command])
        except socket.error, err:
            if hasattr(err, 'errno'):
                if err.errno == errno.EPIPE:
                    raise Exception('EPIPE')
            else:
                if err[1] == "Broken pipe":
                    raise Exception('EPIPE')

        need = len(args_set)
        got = 0
        for line in read_socket(sock, timeout):
            if got < need:
                line = line.strip().split(':', -1)
                if line[0] in args_set:
                    args_dict[line[0]] = line[1]
                    got += 1
            else:
                break

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
                    output("Fatal error. Something bad happend. Check me.")
                    output(err)
                    exit(1)

            cfg_dict_loc[cfg_file] = {'primary_port': '', 'aport': '', 'config': ''}
            for string in file_list:
                if 'primary_port' in string:
                    cfg_dict_loc[cfg_file]['primary_port'] = string.split()[2]
                elif 'admin_port' in string:
                    cfg_dict_loc[cfg_file]['aport'] = string.split()[2]
                cfg_dict_loc[cfg_file]['config'] = cfg_file

    return cfg_dict_loc

def make_proc_dict(adm_port_list, lookup_dict, host='localhost'):
    """ Making dict from running tt\octopus """

    adm_dict_loc = {}

    for aport in adm_port_list:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            open_socket(sock, sock_timeout, host, aport)
            args_dict = get_stats(sock, lookup_dict, sock_timeout)
            args_dict['aport'] = aport
            sock.close()

            filters = {
                'items_used': lambda x: int(str(x).rsplit('.')[0]),
                'arena_used': lambda x: int(str(x).rsplit('.')[0]),
                'recovery_lag': lambda x: int(str(x).rsplit('.')[0]),
                'recovery_run_crc_lag': lambda x: int(str(x).rsplit('.')[0]),
                'config': lambda x: x.strip(' "'),
                'primary_port': lambda x: x.strip(' "'),
            }

            for key in set(args_dict.keys()) & set(filters.keys()):
                if args_dict[key] != '' and args_dict[key] is not 0:
                    args_dict[key] = filters[key](args_dict[key])

            adm_dict_loc[aport] = args_dict
        except Exception, err:
            if 'TO_ERROR' in err:
                adm_dict_loc[aport] = {'aport': aport, 'check_error': "Timeout after %s second" % sock_timeout}
            elif 'ECONNREFUSED' in err:
                adm_dict_loc[aport] = {'aport': aport, 'check_error': "Connection refused"}
            else:
                output(err)
                exit(1)

    return adm_dict_loc

def make_paths_list(paths, excl_pattern, basename=False):
    """ Make a list with paths to files. Full path to cfg and just basename for init scripts """

    paths_list_loc = []
    p = re.compile(excl_pattern)

    for path in paths:
            if basename:
                    path = path.rsplit('/', 1)
                    chdir(path[0])
                    if glob(path[1]):
                            paths_list_loc.extend(glob(path[1]))
            else:
                    if glob(path):
                            paths_list_loc.extend(glob(path))

    paths_list_loc  = [ item for item in paths_list_loc if not p.findall(item) ]
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

def make_port_list(tt_proc_list, pattern):
    """ Parsing tt_proc list to get ports from it """

    p = re.compile(pattern)
    d = re.compile('\d+')
    port_list_loc = []
    for tt_proc in tt_proc_list:
            if p.findall(tt_proc):
                    port = p.findall(tt_proc)[0]
                    port = d.findall(port)[0]
                    port_list_loc.append(port)

    return port_list_loc

def print_alert(check_item, size, limit, aport, error):
    """ Helper fuction to print nice alrts """

    if error != '':
        return 'Octopus/Tarantool with admin port %s runs on error: %s' % (aport, error)
    else:
        return 'Octopus/Tarantool with admin port %s. "%s" is more than %s - %s' % (aport, check_item, limit, size)

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """

    for string in list:
        output(string)

def check_cfg_vs_proc(cfg_dict):
    """ Check configs vs proccesses """

    for cfg in cfg_dict.keys():
            p = re.compile('.*(tarantool|octopus).* adm:.*%s.*' % cfg_dict[cfg]['aport'])
            if not filter(p.match, tt_proc_list):
                    yield "Octopus/Tarantool with config %s is not running!" % cfg

def check_proc_vs_cfg(proc_dict, cfg_dict):
    """ Check proccess vs configs """

    for proc in proc_dict.itervalues():
        if proc['check_error'] != '':
            yield "Octopus/Tarantool with admin port %s runs on error: %s" % (proc['aport'], proc['check_error'])
            continue
        if proc['config'] != '':
            if not proc['config'] in cfg_dict.keys():
                yield "Octopus/Tarantool with admin port %s is running without config!" % proc['aport']
            else:
                if proc['aport'] != cfg_dict[proc['config']]['aport']:
                    yield "Octopus/Tarantool with admin port %s has problem in config: admin port missmatch." % proc['aport']
                elif proc['primary_port'] != cfg_dict[proc['config']]['primary_port']:
                    yield "Octopus/Tarantool with admin port %s has problem in config: primary port missmatch." % proc['aport']
        else:
            yield "Octopus/Tarantool with admin port %s runs on error: Can't get config from process." % proc['aport']

def check_init_vs_chk(init_list, chkcfg_list):
    """ Check init scripts vs chkconfig """

    if not init_list:
        yield "Octopus/Tarantool init scripts not found!"
    else:
        for init in init_list:
            if init != 'octopus' and init != 'tarantool_box' and 'wrapper' not in init:
                p = re.compile(r'^%s\s+.*3:on.*' % init)
                if not filter(p.match, chkcfg_list):
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

    if errors_list:
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
            result_critical.append(print_alert('', '', '', aport, proc_dict[proc]['check_error']))
            continue
        if proc_dict[proc]['items_used'] == '' or proc_dict[proc]['arena_used'] == '':
            result_critical.append(print_alert('', '', '', aport, 'Cant get items_used or arena_used from process.'))
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
    if result_critical and result_warning:
        print_list(result_critical)
        print_list(result_warning)
        exit(1)
    elif result_critical and not result_warning:
        print_list(result_critical)
        exit(1)
    elif result_warning:
        print_list(result_warning)
        exit(2)
    elif result_info:
        print_list(result_info)
        exit(3)

def getip():
    """ Returns ip of this server """

    p = re.compile('mail.ru$')
    p2 = re.compile('i.mail.ru$')
    hostname = socket.gethostname()

    ### Strip ext part of domain name if exist
    if p2.findall(hostname):
        hostname = hostname.rstrip('.mail.ru')
    elif p.findall(hostname):
        hostname = hostname.rstrip('mail.ru')
        hostname += '.i'
    ipaddr = socket.gethostbyname(hostname)

    return ipaddr

def check_pinger(pri_port_list, sec_port_list, memc_port_list, config='/etc/ttmon.conf'):
    """ Check if octopus\tt on this host is in pinger database """

    conf_dict = {}
    pinger_list = []
    port_set = set('')
    ip = getip()

    ### Open conf file and make a dict from it
    try:
        for line in open_file(config):
            line = line.strip()
            if line and not line.startswith("#"):
                (key, val) = line.split()
                conf_dict[key.rstrip(':')] = val
    except Exception, err:
        if 'NO_FILE' in err:
            exit(2)
        elif 'IO_ERROR' in err:
            exit(1)
        else:
            output("Unhandled exeption. Check me.")
            exit(1)

    ### Make a set of ports
    for ports in pri_port_list, sec_port_list, memc_port_list:
        port_set |= set(ports)

    ### Connect to db and check remote_stor_ping table for ip:port on this host
    try:
        db = MySQLdb.connect(host=conf_dict['host'], user=conf_dict['user'], passwd=conf_dict['pass'], db=conf_dict['db'])
        cur = db.cursor()
        for port in port_set:
            cur.execute("SELECT * FROM remote_stor_ping WHERE connect_str='%s:%s';" % (ip, port))
            if int(cur.rowcount) is 0:
                pinger_list.append('Octopus/Tarantool with port %s not found in pinger database!' % port)
    except Exception, err:
            output('MySQL error. Check me.')
            ### We cant print exeption error here 'cos it can contain auth data
            exit(1)

    if pinger_list:
        print_list(pinger_list)
        exit(2)

def check_crc(adm_port_list, proc_dict, crc_lag_limit=2220):
    """ Octopus crc check """

    crc_problems_list = []

    for aport in proc_dict.keys():
        if proc_dict[aport]['wal_feeder_addr'].strip() != '(null)':
            if proc_dict[aport]['recovery_run_crc_status'].strip() != 'ok':
                crc_problems_list.append('Octopus with admin port %s. Difference between master and replica FOUND. CRC32 mismatch. Status is "%s"' % (aport, proc_dict[aport]['recovery_run_crc_status']))
            elif proc_dict[aport]['recovery_run_crc_lag'] > crc_lag_limit:
                crc_problems_list.append('Octopus with admin port %s. "recovery_run_crc_lag" is more than %s - %s' % (aport, crc_lag_limit, proc_dict[aport]['recovery_run_crc_lag']))

    if crc_problems_list:
        print_list(crc_problems_list)
        exit(2)

### Do the work
if opts.type == 'infr_cvp':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:.*\d+')
    cfg_list = make_paths_list(cfg_paths_list, cfg_excl_re)
    cfg_dict = make_cfg_dict(cfg_list)

    ### Check stuff
    check_infrastructure(opts.exit_code, infr_cvp=True)

if opts.type == 'infr_pvc':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:.*\d+')
    cfg_list = make_paths_list(cfg_paths_list, cfg_excl_re)
    cfg_dict = make_cfg_dict(cfg_list)
    proc_dict = make_proc_dict(adm_port_list, general_dict)

    ### Check stuff
    check_infrastructure(opts.exit_code, infr_pvc=True)

if opts.type == 'infr_ivc':
    ### Make stuff
    init_list = make_paths_list(init_paths_list, cfg_excl_re, basename=True)
    chkcfg_list = make_chkcfg_list()

    ### Check stuff
    check_infrastructure(opts.exit_code, infr_ivc=True)

if opts.type == 'slab':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:.*\d+')
    proc_dict = make_proc_dict(adm_port_list, general_dict)

    ### Check stuff
    check_stats(adm_port_list, proc_dict, opts.crit_limit, opts.warn_limit, opts.info_limit)

if opts.type == 'repl':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:.*\d+')
    proc_dict = make_proc_dict(adm_port_list, general_dict)

    ### Check stuff
    check_stats(adm_port_list, proc_dict, opts.crit_limit, opts.warn_limit, opts.info_limit, check_repl=True)

if opts.type == 'pinger':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    sec_port_list = make_port_list(tt_proc_list, ' sec:.*\d+')
    pri_port_list = make_port_list(tt_proc_list, ' pri:.*\d+')
    memc_port_list = make_port_list(tt_proc_list, ' memc:.*\d+')

    ### Check stuff
    check_pinger(pri_port_list, sec_port_list, memc_port_list,  opts.config)

if opts.type == 'octopus_crc':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(octopus_repl_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:.*\d+')
    proc_dict = make_proc_dict(adm_port_list, crc_check_dict)

    ### Check stuff
    check_crc(adm_port_list, proc_dict, crc_lag_limit)
