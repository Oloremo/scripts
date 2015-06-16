#!/usr/bin/env python

import socket
import subprocess
import re
import errno
import MySQLdb
import simplejson as json
from glob import glob
from time import time
from sys import exit, stdout, version_info
from os import chdir, readlink, path, listdir
from select import select
from optparse import OptionParser, OptionGroup
from os.path import isfile, islink
from netifaces import interfaces, ifaddresses

### Gotta catch 'em all!
usage = "usage: %prog -t TYPE [-x port_exlude] [--conf /path/to/conf] [--exit NUM] [--json]"

parser = OptionParser(usage=usage)
parser.add_option('-t', '--type', type='choice', action='store', dest='type',
                  choices=['slab', 'repl', 'infr_cvp', 'infr_pvc', 'infr_ivc', 'pinger', 'octopus_crc', 'backup', 'snaps'],
                  help='Check type. Chose from "slab", "repl", "infr_cvp", "infr_pvc", "infr_ivc", "pinger", "octopus_crc", "backup", "snaps"')
parser.add_option("--json", action="store_true", dest="json_output_enabled",
                  help="Enable json output for some checks")

group = OptionGroup(parser, "Ajusting limits")
group.add_option("-x", type="str", action="append", dest="ex_list", help="Exclude list of ports. This ports won't be cheked by 'pinger' check.")
group.add_option("--exit", dest="exit_code", type="int", default="3", help="Exit code for infrastructure monitoring. Default: 3(Info)")
group.add_option("--conf", dest="config", type="str", default="/etc/ttmon.conf", help="Config file. Used in pinger and backup check. Default: /etc/ttmon.conf")
parser.add_option_group(group)

(opts, args) = parser.parse_args()

### Global vars
cfg_paths_list = ['/usr/local/etc/tarantool*.cfg', '/usr/local/etc/octopus*.cfg', '/etc/tarantool/*.cfg']
cfg_excl_re = 'tarantool.*feeder.*.cfg$'
init_paths_list = ['/etc/init.d/tarantool*', '/etc/init.d/octopus*']
init_exl_list = ['*.rpmsave', 'tarantool_opengraph_feeder', 'tarantool_opengraph', 'octopus', 'octopus-colander', 'tarantool', 'tarantool_box', 'tarantool-initd-wrapper']
proc_pattern = '.*(tarantool|octopus|octopus_rlimit).* adm:.*\d+.*'
octopus_repl_pattern = '.*(octopus: box:hot_standby).* adm:.*\d+.*'
repl_status_list = ['replica/10', 'hot_standby/10']
repl_fail_status = ['/fail:', '/failed']
crc_lag_limit = 2220
general_dict = {'show slab': ['items_used', 'arena_used', 'waste'],
                'show info': ['recovery_lag', 'config', 'status', 'lsn'],
                'show configuration': ['primary_port', 'work_dir', 'wal_writer_inbox_size', 'snap_dir']}
crc_check_dict = {'show configuration': ['wal_feeder_addr'],
                  'show info': ['recovery_run_crc_lag', 'recovery_run_crc_status']}

isEL6 = version_info[0] == 2 and version_info[1] >= 6

### Functions

def output(line):
    if isEL6:
        stdout.write(str(line) + "<br>")
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

def load_config(file, type):
    if not isfile(file):
        print "Config load error. File %s not found." % file
        exit(1)
    try:
        config = json.load(open(file))
        if type in config:
            return config[type]
        else:
            output('Cant load "%s" key from config %s' % (type, file))
            exit(2)
    except Exception, err:
        output("Unhandled exeption. Check me.")
        print err
        exit(1)

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
    args_dict['waste'] = 0
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
                line = line.strip().split(':', 1)
                if line[0] in args_set or line[0] == 'check_error':
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

def make_proc_dict(adm_port_list, lookup_dict, int_conf, host='localhost'):
    """ Making dict from running tt\octopus """

    adm_dict_loc = {}
    sock_timeout = int_conf['sock_timeout'] if 'sock_timeout' in int_conf else 0.1

    for aport in adm_port_list:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            open_socket(sock, sock_timeout, host, aport)
            args_dict = get_stats(sock, lookup_dict, sock_timeout)
            args_dict['aport'] = aport
            sock.close()

            filters = {
                'lsn': lambda x: int(str(x).rsplit('.')[0]),
                'items_used': lambda x: int(str(x).rsplit('.')[0]),
                'arena_used': lambda x: int(str(x).rsplit('.')[0]),
                'waste': lambda x: int(str(x).rsplit('.')[0]),
                'recovery_lag': lambda x: int(str(x).rsplit('.')[0]),
                'recovery_run_crc_lag': lambda x: int(str(x).rsplit('.')[0]),
                'config': lambda x: x.strip(' "'),
                'primary_port': lambda x: x.strip(' "'),
                'wal_writer_inbox_size': lambda x: int(str(x).strip(' "'))
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

def make_paths_list(paths, excl_pattern, basename=False,):
    """ Make a list with paths to files. Full path to cfg and just basename for init scripts """

    paths_list_loc = []
    p = re.compile(excl_pattern)

    for directory in paths:
            if basename:
                    directory = directory.rsplit('/', 1)
                    chdir(directory[0])
                    if glob(directory[1]):
                            paths_list_loc.extend(glob(directory[1]))
            else:
                    if glob(directory):
                            paths_list_loc.extend(glob(directory))

    paths_list_loc = [item for item in paths_list_loc if not p.findall(item)]

    return paths_list_loc

def make_tt_proc_list(pattern):
    """ Making list of a running tt\octopus process to parse after """

    ps = subprocess.Popen(['ps', '-eo' 'args'], stdout=subprocess.PIPE).communicate()[0]
    tt_proc_list_loc = []
    p = re.compile(pattern)
    tt_proc_list_loc = [line for line in ps.splitlines() if p.match(line)]
    return tt_proc_list_loc

def port_to_proc_title_compare(tt_proc_list, ports_set):
    port_title_type = {}
    title_re = re.compile('@([^:\s]+)')

    for port in ports_set:
        memc_re = re.compile('memc: %s' % port)
        for line in tt_proc_list:
            if port in line:
                title = title_re.findall(line)
                title = title[0].strip('@:') if title else 'unknown'
                type = 'octopus' if 'octopus' in line else 'tarantool'
                proto = 'memcached' if memc_re.search(line) else 'iproto'
                port_title_type[port] = {'title': title, 'type': type, 'proto': proto}

    return port_title_type

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
                    port = d.findall(port)[-1]
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

def check_init_vs_chk(init_list, chkcfg_list, init_exl_list):
    """ Check init scripts vs chkconfig """

    good_init_set = set('')

    chdir('/etc/init.d/')
    for init in init_list:
        if islink(init):
            good_init_set.add(readlink(init).split('/')[-1])

    init_exl_list.extend(list(good_init_set))

    for init in init_list:
        if init not in init_exl_list:
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
        for alert in check_init_vs_chk(init_list, chkcfg_list, init_exl_list):
                errors_list.append(alert)

    if errors_list:
            print_list(errors_list)
            exit(exit_code)

def check_stats(proc_dict, config_file, check_repl=False):
    """ Check stats from proccess against limits """

    result_critical = []
    result_warning = []
    result_info = []
    repl_problems = []

    for proc in proc_dict.keys():
        aport = proc_dict[proc]['aport']
        error = proc_dict[proc]['check_error']

        if error != '':
            result_critical.append(print_alert('', '', '', aport, proc_dict[proc]['check_error']))
            continue

        if check_repl:
                config = load_config(config_file, 'repl')
                rep_lag = proc_dict[proc]['recovery_lag']

                if [word for word in repl_fail_status if word in proc_dict[aport]['status']]:
                    repl_problems.append("Octopus with admin port %s. Replication can't connect to master. Status is '%s'" % (aport, proc_dict[aport]['status']))

                if rep_lag == '':
                    result_critical.append(print_alert('', '', '', aport, "Can't get replication lag info. Check me."))
                    continue

                if rep_lag >= config['crit']:
                        result_critical.append(print_alert('replication_lag', rep_lag, config['crit'], aport, error))
                elif rep_lag >= config['warn']:
                        result_warning.append(print_alert('replication_lag', rep_lag, config['warn'], aport, error))
                elif rep_lag >= config['info']:
                        result_info.append(print_alert('replication_lag', rep_lag, config['info'], aport, error))

        else:
                if proc_dict[proc]['items_used'] == '' or proc_dict[proc]['arena_used'] == '':
                    result_critical.append(print_alert('', '', '', aport, 'Cant get items_used or arena_used from process.'))
                    continue

                config = load_config(config_file, 'slab')
                items_used = proc_dict[proc]['items_used']
                arena_used = proc_dict[proc]['arena_used']
                waste = proc_dict[proc]['waste']

                if items_used >= config['crit']:
                        result_critical.append(print_alert('items_used', items_used, config['crit'], aport, error))
                elif items_used >= config['warn']:
                        result_warning.append(print_alert('items_used', items_used, config['warn'], aport, error))
                elif items_used >= config['info']:
                        result_info.append(print_alert('items_used', items_used, config['info'], aport, error))

                if arena_used >= config['crit']:
                        result_critical.append(print_alert('arena_used', arena_used, config['crit'], aport, error))
                elif arena_used >= config['warn']:
                        result_warning.append(print_alert('arena_used', arena_used, config['warn'], aport, error))
                elif arena_used >= config['info']:
                        result_info.append(print_alert('arena_used', arena_used, config['info'], aport, error))

                if waste >= config['waste']:
                        result_info.append(print_alert('waste', waste, config['waste'], aport, error))

    ### Depending on situation it prints revelant list filled with alert strings
    if (result_critical or repl_problems) and result_warning:
        print_list(repl_problems)
        print_list(result_critical)
        print_list(result_warning)
        exit(1)
    elif (result_critical or repl_problems) and not result_warning:
        print_list(repl_problems)
        print_list(result_critical)
        exit(1)
    elif result_warning:
        print_list(result_warning)
        exit(2)
    elif result_info:
        print_list(result_info)
        exit(3)

def getip():
    """ Returns list of ips of this server """

    ip_list = []
    for interface in interfaces():
        if 2 in ifaddresses(interface):
            if ifaddresses(interface)[2][0]['addr'].startswith('10.') and not ifaddresses(interface)[2][0]['addr'].startswith('10.34'):
                ip_list.append(ifaddresses(interface)[2][0]['addr'])

    if not ip_list:
        output("Can't get server ip list. Check me.")
        exit(1)
    else:
        return ip_list

def check_pinger(port_title_type, config_file):
    """ Check if octopus\tt on this host is in pinger database """

    pinger_list = []
    to_json = {}
    ip_list = getip()

    config = load_config(config_file, 'pinger')

    ### Connect to db and check remote_stor_ping table for ip:port on this host
    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'], connect_timeout=1, read_timeout=1)
        cur = db.cursor()
        for ip in ip_list:
            for port in port_title_type.keys():
                cur.execute("SELECT * FROM remote_stor_ping WHERE connect_str='%s:%s' and typ='%s';" % (ip, port, port_title_type[port]['proto']))
                if int(cur.rowcount) is 0:
                    pinger_list.append('Octopus/Tarantool with ip:port %s:%s not found in pinger database!' % (ip, port))
                    to_json[ip + ':' + port] = {'title': port_title_type[port]['title'], 'ip': ip, 'port': port, 'type': port_title_type[port]['type'], 'proto': port_title_type[port]['proto']}
    except Exception, err:
            output('MySQL error. Check me.')
            ### We cant print exeption error here 'cos it can contain auth data
            exit(1)

    if opts.json_output_enabled:
        print json.dumps(to_json)
    elif pinger_list:
        print_list(pinger_list)
        exit(2)

def check_backup(proc_dict, config_file):

    backup_fail_list = []
    config = load_config(config_file, 'backup')
    fqdn = (socket.getfqdn())
    short = fqdn.split('.')[0]
    hostname = short + '.i'
    to_json = {}

    for instance in proc_dict.keys():
        if proc_dict[instance]['check_error'] != '':
            backup_fail_list.append("Octopus/Tarantool with admin port %s runs on error: %s" % (proc_dict[instance]['aport'], proc_dict[instance]['check_error']))
            continue

        status = proc_dict[instance]['status'].lstrip()
        title = proc_dict[instance]['title']
        wd = proc_dict[instance]['work_dir'].strip('" ')
        wd_snaps = wd + '/snaps'
        wd_xlogs = wd + '/xlogs'
        if islink(wd_snaps):
            wd_snaps_orig = readlink(wd_snaps)
        if islink(wd_xlogs):
            wd_xlogs_orig = readlink(wd_xlogs)

        try:
            db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'])
            cur = db.cursor()
            if 'primary' in status and proc_dict[instance]['wal_writer_inbox_size'] != 0:
                cur.execute("select * from server_backups where host = '%s' and (tarantool_snaps_dir='%s' or tarantool_snaps_dir='%s') and (tarantool_xlogs_dir='%s' or tarantool_xlogs_dir='%s') and skip_backup=0" % (hostname, wd_snaps, wd_snaps_orig, wd_xlogs, wd_xlogs_orig))
            elif 'primary' in status and proc_dict[instance]['wal_writer_inbox_size'] is 0:
                cur.execute("select * from server_backups where host = '%s' and (tarantool_snaps_dir='%s' or tarantool_snaps_dir='%s') and skip_backup=0" % (hostname, wd_snaps, wd_snaps_orig))
            elif [pattern for pattern in repl_status_list if status.startswith(pattern)] and proc_dict[instance]['wal_writer_inbox_size'] != 0:
                cur.execute("select * from server_backups where host = '%s' and (tarantool_snaps_dir='%s' or tarantool_snaps_dir='%s') and (tarantool_xlogs_dir='%s' or tarantool_xlogs_dir='%s')" % (hostname, wd_snaps, wd_snaps_orig, wd_xlogs, wd_xlogs_orig))
            elif [pattern for pattern in repl_status_list if status.startswith(pattern)] and proc_dict[instance]['wal_writer_inbox_size'] is 0:
                cur.execute("select * from server_backups where host = '%s' and (tarantool_snaps_dir='%s' or tarantool_snaps_dir='%s')" % (hostname, wd_snaps, wd_snaps_orig))
            if int(cur.rowcount) is 0:
                backup_fail_list.append("Octopus/Tarantool with config %s not found in backup database!" % (proc_dict[instance]['config']))
                type = 'octopus' if 'octopus' in status else 'tarantool'
                replica = True if [pattern for pattern in repl_status_list if status.startswith(pattern)] else False
                to_json[proc_dict[instance]['aport']] = {'title': title, 'type': type, 'snaps': wd_snaps, 'xlogs': wd_xlogs, 'work_dir': wd, 'replica': replica}
        except Exception, err:
                output('MySQL error. Check me.')
                ### We cant print exeption error here 'cos it can contain auth data
                #print err
                exit(1)

    if opts.json_output_enabled:
        print json.dumps(to_json)
    elif backup_fail_list:
        print_list(backup_fail_list)
        exit(2)

def check_crc(adm_port_list, proc_dict, crc_lag_limit=2220):
    """ Octopus crc check """

    crc_problems_list = []

    for aport in proc_dict.keys():
        ### We do not check it on masters and if ignore_run_crc=1
        if proc_dict[aport]['wal_feeder_addr'].strip() != '(null)' and proc_dict[aport]['recovery_run_crc_status'].strip() != '':
            if proc_dict[aport]['recovery_run_crc_status'].strip() != 'ok':
                crc_problems_list.append('Octopus with admin port %s. Difference between master and replica FOUND. CRC32 mismatch. Status is "%s"' % (aport, proc_dict[aport]['recovery_run_crc_status']))
            elif proc_dict[aport]['recovery_run_crc_lag'] > crc_lag_limit:
                crc_problems_list.append('Octopus with admin port %s. "recovery_run_crc_lag" is more than %s - %s' % (aport, crc_lag_limit, proc_dict[aport]['recovery_run_crc_lag']))

    if crc_problems_list:
        print_list(crc_problems_list)
        exit(2)

def check_snaps(proc_dict, config_file):
    problems = []
    config = load_config(config_file, 'snaps')

    for inst in proc_dict.values():
        if inst['check_error'] != '':
            problems.append("Octopus/Tarantool with admin port %s runs on error: %s" % (inst['aport'], inst['check_error']))
            continue
        dir = inst['snap_dir'].strip('" ')
        if path.exists(dir):
            snap_dir = readlink(dir) if islink(dir) else dir
            chdir(snap_dir)
            if listdir(snap_dir):
                newest = max(listdir(snap_dir), key=path.getmtime)
                snap_lsn = int(newest.split('.')[0])
                if time() - path.getmtime(newest) > (config['limit'] * 60) and inst['lsn'] > snap_lsn:
                    problems.append("Octopus/Tarantool with snap dir %s. Last snapshot was made more than %s minutes ago." % (dir, config['limit']))
            else:
                problems.append("Octopus/Tarantool with snap dir %s runs on error: no snapshot found" % dir)
        else:
            problems.append("Octopus/Tarantool with snap dir %s runs on error: not such directory. Its bug in monitoring or huge fuckup on server." % dir)

    if problems:
        print_list(problems)
        exit(2)

### Load internal config
int_conf = load_config(opts.config, 'internal')

### Do the work
if opts.type == 'infr_cvp':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:\s*\d+')
    cfg_list = make_paths_list(cfg_paths_list, cfg_excl_re)
    cfg_dict = make_cfg_dict(cfg_list)

    ### Check stuff
    check_infrastructure(opts.exit_code, infr_cvp=True)

if opts.type == 'infr_pvc':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:\s*\d+')
    cfg_list = make_paths_list(cfg_paths_list, cfg_excl_re)
    cfg_dict = make_cfg_dict(cfg_list)
    proc_dict = make_proc_dict(adm_port_list, general_dict, int_conf)

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
    adm_port_list = make_port_list(tt_proc_list, ' adm:\s*\d+')
    proc_dict = make_proc_dict(adm_port_list, general_dict, int_conf)

    ### Check stuff
    check_stats(proc_dict, opts.config)

if opts.type == 'repl':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:\s*\d+')
    proc_dict = make_proc_dict(adm_port_list, general_dict, int_conf)

    ### Check stuff
    check_stats(proc_dict, opts.config, check_repl=True)

if opts.type == 'pinger':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    sec_port_list = make_port_list(tt_proc_list, ' sec:\s*\d+')
    pri_port_list = make_port_list(tt_proc_list, ' pri:\s*\d+')
    memc_port_list = make_port_list(tt_proc_list, 'primary.*memc:\s*\d+')

    ports_set = set('')
    for ports in pri_port_list, sec_port_list, memc_port_list:
        ports_set |= set(ports)
    if opts.ex_list:
        ports_set.difference_update(set(opts.ex_list))

    port_title = port_to_proc_title_compare(tt_proc_list, ports_set)

    ### Check stuff
    check_pinger(port_title, opts.config)

if opts.type == 'backup':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:\s*\d+')
    proc_dict = make_proc_dict(adm_port_list, general_dict, int_conf)

    ports_set = set('')
    ports_set |= set([port for port in proc_dict.keys()])

    port_title = port_to_proc_title_compare(tt_proc_list, ports_set)
    for inst in proc_dict.keys():
        proc_dict[inst]['title'] = port_title[inst]['title']

    ### Check stuff
    check_backup(proc_dict, opts.config)

if opts.type == 'octopus_crc':
    ### Make stuff
    tt_proc_list = make_tt_proc_list(octopus_repl_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:\s*\d+')
    proc_dict = make_proc_dict(adm_port_list, crc_check_dict, int_conf)

    ### Check stuff
    check_crc(adm_port_list, proc_dict, crc_lag_limit)

if opts.type == 'snaps':
    tt_proc_list = make_tt_proc_list(proc_pattern)
    adm_port_list = make_port_list(tt_proc_list, ' adm:\s*\d+')
    proc_dict = make_proc_dict(adm_port_list, general_dict, int_conf)

    check_snaps(proc_dict, opts.config)
