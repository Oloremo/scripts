#!/usr/bin/env python

import socket
import MySQLdb
import subprocess
import simplejson as json
import random
import requests
from optparse import OptionParser
from os.path import isfile, islink
from os import readlink

### Gotta catch 'em all!
usage = "usage: %prog "

parser = OptionParser(usage=usage)
parser.add_option('-t', '--type', type='choice', action='store', dest='type',
                  choices=['mysql', 'tt', 'memc'],
                  help='Backup type. Chose from "mysql", "tt", "memc"')
parser.add_option('-m', '--mode', type='choice', action='store', dest='mode',
                  choices=['pinger', 'backup'],
                  help='Action mode. Chose from "pinger" and "backup"')
parser.add_option("--conf", dest="config", type="str", default="/etc/hal9000.conf", help="Config file. Default: /etc/hal9000.conf")
parser.add_option("-b", action="store_true", dest="batch", help="Enable batch mode")
parser.add_option("--bull", action="store", dest="bull", help="Specify bull host for backup")

(opts, args) = parser.parse_args()

if not opts.mode or not opts.type:
    print "You must chose type of action as -t and mode of action as -m"
    usage
    exit(1)

if not opts.bull and opts.batch and opts.mode == 'backup':
    print "In batch mode you must specify bull host for backup. For example: '--bull bull40.i'"
    exit(1)

def load_config(file, type):
    if not isfile(file):
        print "Config load error. File %s not found." % file
        exit(1)
    try:
        config = json.load(open(file))
        if type in config:
            return config[type]
        else:
            print 'Cant load "%s" key from config %s' % (type, file)
            exit(2)
    except Exception, err:
        print "Error while load config from %s. Unhandled exeption. Check me." % file
        print err
        exit(1)

def yes_no():
    yes = set(['yes', 'y'])
    no = set(['no', 'n'])

    while True:
        choice = raw_input('Should we proceed? Type "yes" or "no":  ').lower()
        if choice in yes:
            return True
        elif choice in no:
            exit(0)
        else:
            print "Please respond with 'yes' or 'no'"

def get_tt_json(type):

    script = '/etc/snmp/bin/ttmon.py'
    if not isfile(script):
        print 'File "%s" not found' % script
        exit(1)
    result = subprocess.Popen([script, '-t', type, '--json'], stdout=subprocess.PIPE).communicate()[0]
    if result:
        return json.loads(result)
    else:
        exit(0)

def get_memc_json():

    script = '/etc/snmp/bin/memc.py'
    if not isfile(script):
        print 'File "%s" not found' % script
        exit(1)
    result = subprocess.Popen([script, '--json'], stdout=subprocess.PIPE).communicate()[0]
    if result:
        return json.loads(result)
    else:
        exit(0)

def get_mysql_json(type):

    script = '/etc/snmp/bin/mysql.py'
    if not isfile(script):
        print 'File "%s" not found' % script
        exit(1)
    result = subprocess.Popen([script, '-t', type, '--json'], stdout=subprocess.PIPE).communicate()[0]
    if result:
        return json.loads(result)
    else:
        exit(0)

def get_creds_from_oc():

    script = '/usr/local/bin/fetch-cred-from-oc.py'
    if not isfile(script):
        print 'File "%s" not found' % script
        exit(1)
    result = subprocess.Popen([script], stdout=subprocess.PIPE).communicate()[0]
    if result:
        return json.loads(result)
    else:
        exit(0)

def print_insert_data(names_list, data_list, type):
    data_dict = dict(zip(names_list, data_list))
    name_max_len = len(max(names_list, key=len))

    print "\nYou're about to add this data to %s:\n" % type
    for name in names_list:
        print "%s: %s" % (name.ljust(name_max_len + 5), data_dict[name])
    print '\n'

    if not opts.batch:
        yes_no()

def get_bull():
    r = requests.get('http://doll.i:9999/get-bull')
    if r.status_code == 200:
        bull_json = r.json()
    else:
        print "Most free bull host fetch error"
        print 'http://doll.i:9999/get-bull status code is %s' % r.status_code
        exit(1)

    ten_plus = dict((key, value) for (key, value) in bull_json.iteritems() if value > 10000000000)
    five_plus = dict((key, value) for (key, value) in bull_json.iteritems() if value > 5000000000)
    one_plus = dict((key, value) for (key, value) in bull_json.iteritems() if value > 1000000000)

    if ten_plus:
        top5_bulls = sorted(ten_plus, key=ten_plus.get, reverse=True)[:5]
        return str(random.choice(top5_bulls))
    elif five_plus:
        top5_bulls = sorted(ten_plus, key=ten_plus.get, reverse=True)[:5]
        return str(random.choice(top5_bulls))
    elif one_plus:
        top_bull = max(one_plus, key=one_plus.get)
        return str(top_bull)
    else:
        print "There is no bull with free space more than 1Tb. Refuse to choose."
        exit(1)

def mysql_execute(config, select_tmpl, select_data, insert_tmpl, insert_data):
    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'])
        cur = db.cursor()
        ### Check if it's allready exist
        cur.execute(select_tmpl, select_data)
        if int(cur.rowcount) is not 0:
            print "Record for this instance allready exist"
        else:
            cur.execute(insert_tmpl, insert_data)
            db.commit()
            print "Success!"
    except Exception, err:
            db.rollback()
            print 'MySQL error.'
            print err
            exit(1)

def add_backup(config_file, type, skip_backup=0, backup_retention=14, machine_retention=1, gzip_period=3):

    config = load_config(config_file, 'backup')
    fqdn = (socket.getfqdn())
    short = fqdn.split('.')[0]
    hostname = short + '.i'

    if type == 'tt':
        bk_dict = get_tt_json('backup')
        bk_type = 'tarantool'
        select_tmpl = "select * from server_backups where host = %s and (tarantool_snaps_dir = %s or tarantool_snaps_dir = %s)"

        for inst in bk_dict.keys():
            wd = bk_dict[inst]['work_dir'].strip('" ')
            wd_snaps = wd + '/snaps'
            wd_snaps_orig = readlink(wd_snaps) if islink(wd_snaps) else wd_snaps

            if not bk_dict[inst]['replica']:
                skip_backup = 0
                insert_tmpl = "insert into backup.server_backups (host, type, rsync_host, rsync_modulepath, backup_retention, machine_retention, gzip_period, tarantool_snaps_dir, tarantool_xlogs_dir, skip_backup, optfile_list) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '')"
                names_list = ['hostname', 'rsync_host', 'type', 'module', 'snaps_dir', 'xlogs_dir', 'backup_retention', 'machine_retention', 'skip_backup']
                rsync_module = 'my_backup/%s/%s/%s' % (bk_dict[inst]['type'], short, bk_dict[inst]['inst_name'])
                rsync_host = get_bull()

                data_list = [hostname, rsync_host, bk_type, rsync_module, bk_dict[inst]['snaps'], bk_dict[inst]['xlogs'], backup_retention, machine_retention, skip_backup]
                print_insert_data(names_list, data_list, 'backup')

                select_data = (hostname, wd_snaps, wd_snaps_orig)
                insert_data = (hostname, bk_type, rsync_host, rsync_module, backup_retention, machine_retention, gzip_period, bk_dict[inst]['snaps'], bk_dict[inst]['xlogs'], skip_backup)
            else:
                skip_backup = 1
                insert_tmpl = "insert into backup.server_backups (host, type, machine_retention, tarantool_snaps_dir, tarantool_xlogs_dir, skip_backup, optfile_list) values (%s, %s, %s, %s, %s, %s, '')"
                names_list = ['hostname', 'type', 'snaps_dir', 'xlogs_dir', 'machine_retention', 'skip_backup']

                data_list = [hostname, bk_type, bk_dict[inst]['snaps'], bk_dict[inst]['xlogs'], machine_retention, skip_backup]
                print_insert_data(names_list, data_list, 'backup')

                select_data = (hostname, wd_snaps, wd_snaps_orig)
                insert_data = (hostname, bk_type, machine_retention, bk_dict[inst]['snaps'], bk_dict[inst]['xlogs'], skip_backup)
            mysql_execute(config, select_tmpl, select_data, insert_tmpl, insert_data)

    if type == 'mysql':
        bk_dict = get_mysql_json('backup')
        select_tmpl = "select * from backup.server_backups where host = %s and mysql_backup_dir = %s"
        insert_tmpl = "insert into backup.server_backups (host, type, rsync_host, rsync_modulepath, backup_retention, mysql_backup_vol, mysql_backup_dir, mysql_sock, mysql_initscript, optfile_list) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, '');"
        names_list = ['hostname', 'rsync_host', 'rsync_module', 'type', 'mysql_initscript', 'mysql_sock', 'mysql_backup_vol', 'mysql_backup_dir']

        for inst in bk_dict.keys():
            rsync_host = get_bull()
            mysql_initscript = bk_dict[inst]['mysql_initscript']
            mysql_sock = bk_dict[inst]['mysql_sock']
            mysql_backup_vol = bk_dict[inst]['mysql_backup_vol']
            mysql_backup_dir = bk_dict[inst]['mysql_backup_dir']
            rsync_module = 'my_backup/mysql'

            data_list = [hostname, rsync_host, rsync_module, type, mysql_initscript, mysql_sock, mysql_backup_vol, mysql_backup_dir]
            print_insert_data(names_list, data_list, 'backup')

            select_data = (hostname, mysql_backup_dir)
            insert_data = (hostname, type, rsync_host, rsync_module, backup_retention, mysql_backup_vol, mysql_backup_dir, mysql_sock, mysql_initscript)
            mysql_execute(config, select_tmpl, select_data, insert_tmpl, insert_data)

def add_pinger(config_file, type):
    config = load_config(config_file, 'pinger')

    if type == 'tt':
        ping_dict = get_tt_json('pinger')
        names_list = ['title', 'conn_string', 'type', 'proto']
        select_tmpl = "select * from remote_stor_ping where connect_str = %s and typ = %s"
        insert_tmpl = "insert into remote_stor_ping values (%s, %s, '4', '', '', %s, NULL, NULL)"

        for inst in ping_dict.values():
            title = '%s-%s:%s' % (inst['title'], inst['ip'], inst['port'])
            conn_string = '%s:%s' % (inst['ip'], inst['port'])
            type = inst['type']
            proto = inst['proto']

            data_list = [title, conn_string, type, proto]
            print_insert_data(names_list, data_list, 'pinger')

            select_data = (conn_string, 'iproto')
            insert_data = (title, proto, conn_string)
            mysql_execute(config, select_tmpl, select_data, insert_tmpl, insert_data)
    elif type == 'memc':
        ping_dict = get_memc_json()
        names_list = ['title', 'conn_string', 'proto']
        select_tmpl = "select * from remote_stor_ping where connect_str = %s and typ = %s"
        insert_tmpl = "insert into remote_stor_ping values (%s, %s, '4', '', '', %s, NULL, NULL)"

        for inst in ping_dict.values():
            title = 'memcached-%s-%s:%s' % (inst['title'], inst['ip'], inst['port'])
            conn_string = '%s:%s' % (inst['ip'], inst['port'])
            proto = inst['proto']

            data_list = [title, conn_string, proto]
            print_insert_data(names_list, data_list, 'pinger')

            select_data = (conn_string, 'memcached')
            insert_data = (title, proto, conn_string)
            mysql_execute(config, select_tmpl, select_data, insert_tmpl, insert_data)
    elif type == 'mysql':
        mysql_dict = get_mysql_json('pinger')
        cred_dict = get_creds_from_oc()

        names_list = ['title', 'ip', 'port', 'ro']
        select_tmpl = "select * from remote_stor_ping where connect_str like %s and typ = %s and request like %s"
        insert_tmpl = "insert into remote_stor_ping values (%s, %s, '6', %s, '1', %s, NULL, NULL)"

        for inst in mysql_dict.values():
            title = 'mysql-%s-%s:%s' % (inst['title'], inst['ip'], inst['port'])
            base = cred_dict[inst['title']]['base']
            user = cred_dict[inst['title']]['user']
            password = cred_dict[inst['title']]['pass']
            conn_string = 'dbi:mysql:%s:%s,%s,%s' % (base, inst['ip'], user, password)
            request = 'select 1' if inst['ro'] else 'update ping_test set val=2;update ping_test set val=1;select 1'

            data_list = [title, inst['ip'], inst['port'], inst['ro']]
            print_insert_data(names_list, data_list, 'pinger')

            select_data = (conn_string, 'dbi', request)
            insert_data = (title, 'dbi', request, conn_string)
            mysql_execute(config, select_tmpl, select_data, insert_tmpl, insert_data)

if opts.mode == 'pinger':
    add_pinger(opts.config, opts.type)
if opts.mode == 'backup':
    add_backup(opts.config, opts.type)
