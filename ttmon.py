#!/usr/bin/env python

import socket
import subprocess
import re
from glob import glob
from sys import exit
from os import chdir
from select import select
from optparse import OptionParser, OptionGroup
from os.path import isfile                # for OS file check

### Gotta catch 'em all!
usage = "usage: %prog -t TYPE [-c LIMIT] [-w LIMIT] [-i LIMIT] [--exit NUM]"

parser = OptionParser(usage=usage)
parser.add_option('-t', '--type', type='choice', action='store', dest='type', choices=['slab', 'repl', 'infr_cvp' , 'infr_pvc', 'infr_ivc'],
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
proc_pattern = '.*(tarantool|octopus).*adm:.?\d+.*'

### Functions
def open_file(filename):
    """ We try to open file and copy it into list. """
    if not isfile(filename):
        print "I/O error. There is no '%s' but we find it before by glob. Check me." % filename
        raise Exception('NO_FILE')
    try:
        return list(open(filename))
    except IOError, error:
        print "I/O error. Can't open file '%s'. Check me." % filename
        print "I/O error({0}): {1}".format(error.errno, error.strerror)
        raise Exception('IO_ERROR')
    except:
        raise Exception

def read_socket(sock, recv_buffer=4096):
        """ Nice way to read from socket. We use select() for timeout handling """
        buffer = ''
        receiving = True
        while receiving:
                ready = select([sock], [], [], 1)
                if ready[0]:
                        data = sock.recv(recv_buffer)
                        buffer += data

                        for line in buffer.splitlines():
                                if 'config:' in line:
                                        receiving = False
        for line in buffer.splitlines():
                yield line

def get_stats(sock, command, *arg):
        """ Parsing internal tt\octopus info from admin port """
        result_list = []
        sock.sendall(command)

        for line in read_socket(sock):
                for my_arg in arg:
                        if my_arg in line:
                                line = line.split()
                                my_arg = line[1]
                                result_list.append(my_arg)
        return result_list

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

                for string in file_list:
                        if 'primary_port' in string:
                                string = string.split()
                                pport = string[2]
                        elif 'admin_port' in string:
                                string = string.split()
                                aport = string[2]

                cfg_dict_loc[cfg_file] = {'pport': pport, 'aport': aport}

        return cfg_dict_loc

def make_proc_dict(adm_port_list, host='localhost'):
        """ Making dict from running tt\octopus """
        adm_dict_loc = {}
        for aport in adm_port_list:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0)
                sock.connect((host, int(aport)))

                items_used, arena_used, rep_lag, config = get_stats(sock, 'show slab\nshow info\n', 'items_used', 'arena_used', 'recovery_lag', 'config')
                sock.close()

                ### FIXME
                items_used, arena_used, rep_lag, config = int(float(items_used.rstrip('%'))), int(float(arena_used.rstrip('%'))), int(float(rep_lag)), config.strip('"')
                adm_dict_loc[config] = {'aport': aport, 'items_used': items_used, 'arena_used': arena_used, 'rep_lag': rep_lag}

        return adm_dict_loc

def make_paths_list(paths, basename=False):
        """ Make a list with paths to files. Full path to cfg and just basename for init scripts """
        paths_list_loc = []
        for path in paths:
                if basename:
                        path = path.rsplit('/', 1)
                        chdir(path[0])
                        if len(glob(path[1])) != 0:
                                paths_list_loc.append(glob(path[1]))
                else:
                        if len(glob(path)) != 0:
                                paths_list_loc.append(glob(path))

        return paths_list_loc[0]

def make_tt_proc_list(pattern):
        """ Making list of a running tt\octopus process to parse after """
        ps = subprocess.Popen(['ps', 'axw'], stdout=subprocess.PIPE).communicate()[0]
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
        p = re.compile('adm:.?\d+')
        d = re.compile('\d+')
        adm_port_list_loc = []
        for tt_proc in tt_proc_list:
                if p.findall(tt_proc):
                        aport = p.findall(tt_proc)[0]
                        aport = d.findall(aport)[0]
                        adm_port_list_loc.append(aport)

        return adm_port_list_loc

def print_alert(check_item, size, limit, aport):
                return 'Octopus/Tarantool with admin port %s. "%s" is more than %d - %d' % (aport, check_item, limit, size)

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """
    for string in list:
        print string

def check_cfg_vs_proc(cfg_dict):
        for cfg in cfg_dict.keys():
                p = re.compile('.*(tarantool|octopus).*adm:.?%s.*' % cfg_dict[cfg]['aport'])
                if not filter(p.match, tt_proc_list):
                        yield "Octopus/Tarantool with config %s is not running!" % cfg

def check_proc_vs_cfg(proc_dict, cfg_dict):
        for proc_cfg in proc_dict.iterkeys():
                if not proc_cfg in cfg_dict.keys():
                        yield "Octopus/Tarantool with admin port %s is running without config!" % proc_dict[proc_cfg]['aport']


def check_init_vs_chk(init_list, chkcfg_list):
        for init in init_list:
                p = re.compile(r'^%s.*3:on.*' % init)
                if not filter(p.search, chkcfg_list):
                        yield 'Init script "%s" is not added to chkconfig!' % init

def check_infrastructure(exit_code, infr_cvp=False, infr_pvc=False, infr_ivc=False):
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
        result_critical = []
        result_warning = []
        result_info = []

        for proc in proc_dict.keys():
                items_used = proc_dict[proc]['items_used']
                arena_used = proc_dict[proc]['arena_used']
                aport = proc_dict[proc]['aport']
                rep_lag = proc_dict[proc]['rep_lag']

                if check_repl:
                        if rep_lag >= crit:
                                result_critical.append(print_alert('replication_lag', rep_lag, crit, aport))
                        elif rep_lag >= warn:
                                result_warning.append(print_alert('replication_lag', rep_lag, warn, aport))
                        elif rep_lag >= info:
                                result_info.append(print_alert('replication_lag', rep_lag, info, aport))
                else:
                        if items_used >= crit:
                                result_critical.append(print_alert('items_used', items_used, crit, aport))
                        elif items_used >= warn:
                                result_warning.append(print_alert('items_used', items_used, warn, aport))
                        elif items_used >= info:
                                result_info.append(print_alert('items_used', items_used, info, aport))

                        if arena_used >= crit:
                                result_critical.append(print_alert('arena_used', arena_used, crit, aport))
                        elif arena_used >= warn:
                                result_warning.append(print_alert('arena_used', arena_used, warn, aport))
                        elif arena_used >= info:
                                result_info.append(print_alert('arena_used', arena_used, info, aport))
       
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

### make stuff
#tt_proc_list = make_tt_proc_list(proc_pattern)
#cfg_list = make_paths_list(cfg_paths_list)
#init_list = make_paths_list(init_paths_list, basename=True)
#cfg_dict = make_cfg_dict(cfg_list)
#adm_port_list = make_adm_port_list(tt_proc_list)
#proc_dict = make_proc_dict(adm_port_list)
#chkcfg_list = make_chkcfg_list()

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
        check_stats(adm_port_list, proc_dict, opts.crit_limit, opts.warn_limit, opts.info_limit,  check_repl=True)
