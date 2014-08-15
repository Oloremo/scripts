#!/usr/bin/env python

from sys import exit, stdout, version_info  # for exit code, output func and version check
from optparse import OptionParser           # for usage
from glob import glob                       # for fs file paths
from os.path import isfile                  # for OS file check
import MySQLdb                              # for mysql
from os import chdir                        # for glob()
import socket                               # for network
import os.path                              # for mtime check
import time                                 # for mtime check

### Gotta catch 'em all!
usage = "usage: %prog -t TYPE [-c LIMIT] [-f FLAG]"

parser = OptionParser(usage=usage)
parser.add_option('-t', '--type', type='choice', action='store', dest='type',
                  choices=['ok', 'repl', 'load'],
                  help='Check type. Chose from "ok", "repl", "load"')
parser.add_option("-c", "--crit", type="int", dest="crit_limit",
                  help="Critical limit. Default: 100 for 'load' and 600 for 'repl'")

(opts, args) = parser.parse_args()

if opts.type == 'load':
        if not opts.crit_limit:
                opts.crit_limit = 100
elif opts.type == 'repl':
        if not opts.crit_limit:
                opts.crit_limit = 600

### Global vars
mysql_init_path = ['mysql-*']
lookup_list = ['datadir', 'socket']

### Version check
isEL6 = version_info[0] == 2 and version_info[1] >= 6

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

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """

    for string in list:
        output(string)

def get_all_mysql(mysql_init_path):
    """ Making list of all mysql init scripts """

    inits = []
    chdir('/etc/init.d')
    for path in mysql_init_path:
        if glob(path):
            inits.extend(glob(path))

    return inits

def make_mysql_dict(inits, lookup_list):
    """ Make dict of all mysql with datadir and socket args """

    chdir('/etc/init.d')
    mysql_dict = {}

    for init in inits:
        mysql_args_dict = {}

        mysql_args_dict['db'] = init.partition('-')[2]
        file = open_file(init)
        for line in file:
            line = line.split('=')
            if line[0] in lookup_list:
                mysql_args_dict[line[0]] = line[1].strip()
        mysql_dict[init] = mysql_args_dict

    return mysql_dict

def check_flag(mysql_dict):

    result_dict = {}

    for inst in mysql_dict.keys():
        flag = '/var/tmp/mysql-backup-tmp-%s.sock.flag' % inst
        result_dict[inst] = {'flag': False, 'stale': False, 'file': flag}

        if isfile(flag):
            result_dict[inst]['flag'] = True
            cut_time = time.time()
            bk_mtime = os.path.getmtime(flag)
            if cut_time - bk_mtime > 3600:
                result_dict[inst]['stale'] = True

    return result_dict

def check_ok(mysql_dict, flag_dict):
    """ Check if mysql err file ends with 'OK' """

    hostname = socket.gethostname()
    result = []

    for inst in mysql_dict.keys():
        if flag_dict[inst]['flag'] and flag_dict[inst]['stale']:
            result.append('Stale backup flag found! %s is older than 60 min.' % flag_dict[inst]['file'])
        if not flag_dict[inst]['flag']:
            chdir(mysql_dict[inst]['datadir'])
            file = open_file(hostname + '.err')
            if file[-1].strip() != 'OK':
                result.append('Mysql with datadir "%s" has problems: %s' % (mysql_dict[inst]['datadir'], file[1].strip()))

    if result:
        print_list(result)
        exit(2)

def check_mysql(mysql_dict, flag_dict, crit, check_repl=False, check_load=False):
    """ Check replica lag or mysql proc count """

    result_critical = []
    result_warning = []

    for inst in mysql_dict.keys():
        if flag_dict[inst]['flag'] and flag_dict[inst]['stale']:
            result_warning.append('Stale backup flag found! %s is older than 60 min.' % flag_dict[inst]['file'])
        if not flag_dict[inst]['flag']:
            try:
                db = MySQLdb.connect(unix_socket=mysql_dict[inst]['socket'])
                cur = db.cursor()

                if check_repl:
                    cur.execute("show slave status")
                    ### We came to agreement what if where is nothing in "show slave status" we assume what it's master
                    if cur.rowcount == 0:
                        exit(0)

                    ### Get values
                    row = cur.fetchone()
                    ### Get keys
                    field_names = [i[0] for i in cur.description]
                    ### Make dict
                    slave_dict = dict(zip(field_names, row))

                    if slave_dict['Slave_IO_Running'] == 'Yes' and slave_dict['Slave_SQL_Running'] == 'Yes':
                        if slave_dict['Seconds_Behind_Master'] >= crit:
                            result_critical.append('Mysql with datadir %s: Seconds_Behind_Master is more than %s - %s' % (mysql_dict[inst]['datadir'], crit, slave_dict['Seconds_Behind_Master']))
                    else:
                        result_critical.append('Mysql with datadir %s: replication is not running' % (mysql_dict[inst]['datadir']))
                elif check_load:
                    ### Works from Mysql 5.1.7
                    proc_count = cur.execute("SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST where COMMAND != 'Sleep'")
                    long_proc = cur.execute("SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST where COMMAND != 'Sleep' and TIME > 5 ")
                    ### if proc count above critical AND 20% of procs runs longer than 5 sec...
                    if int(proc_count) >= crit and int(long_proc) > (crit / 100) * 20:
                            result_critical.append('Mysql with datadir %s: process count is more than %s - %s' % (mysql_dict[inst]['datadir'], crit, cur.rowcount))
            except Exception, err:
                output('Mysql monitoring error. Check me.')
                print err
                exit(1)

    ### Print result
    if result_critical and result_warning:
        print_list(result_critical)
        print_list(result_warning)
        exit(1)
    elif result_critical and result_warning:
        print_list(result_critical)
        exit(1)
    elif result_warning:
        print_list(result_warning)
        exit(2)

### Make depended things
inits = get_all_mysql(mysql_init_path)
mysql_dict = make_mysql_dict(inits, lookup_list)
flag_dict = check_flag(mysql_dict)

### Check things
if opts.type == 'ok':
    check_ok(mysql_dict, flag_dict)
elif opts.type == 'repl':
    check_mysql(mysql_dict, flag_dict, opts.crit_limit, check_repl=True)
elif opts.type == 'load':
    check_mysql(mysql_dict, flag_dict, opts.crit_limit, check_load=True)
