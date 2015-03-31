#!/usr/bin/env python

import socket
import MySQLdb
import subprocess
import simplejson as json
import random
import requests
from optparse import OptionParser
from os.path import isfile

### Gotta catch 'em all!
usage = "usage: %prog "

parser = OptionParser(usage=usage)
parser.add_option('-t', '--type', type='choice', action='store', dest='type',
                  choices=['mysql', 'tarantool', 'octopus'],
                  help='Backup type. Chose from "mysql", "tarantool", "octopus"')
parser.add_option("--conf", dest="config", type="str", default="/etc/hal9000.conf", help="Config file. Default: /etc/hal9000.conf")
parser.add_option("-b", action="store_true", dest="batch", help="Enable batch mode")
parser.add_option("--auto", action="store_true", dest="auto", help="Enable auto mode")
parser.add_option("--bull", action="store", dest="bull", help="Specify bull host for backup")

(opts, args) = parser.parse_args()

if not opts.auto and not opts.bull and opts.batch:
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
        print "Error while load config from %s" % file
        print "Unhandled exeption. Check me."
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
    tt_json = subprocess.Popen(['/etc/snmp/bin/ttmon.py', '-t', type, '--json'], stdout=subprocess.PIPE).communicate()[0]
    return json.loads(tt_json)

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
        print "There is no bull with free space more than 1Tb. Refuse to choose"
        exit(1)

def backup_mysql_execute(config, insert_tmpl, hostname, bk_type, rsync_host, module, rsync_user, backup_retention, machine_retention, gzip_period, snaps_dir, xlogs_dir, skip_check, skip_backup):
    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'])
        cur = db.cursor()
        ### Check if it's allready exist
        cur.execute("select * from server_backups where host = '%s' and tarantool_snaps_dir='%s' and tarantool_xlogs_dir='%s'" % (hostname, snaps_dir, xlogs_dir))
        if int(cur.rowcount) is not 0:
            print "Record for this instance allready exist"
        else:
            cur.execute("%s ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '');" % (insert_tmpl, hostname, bk_type, rsync_host, module, rsync_user, backup_retention, machine_retention, gzip_period, snaps_dir, xlogs_dir, skip_check, skip_backup))
            db.commit()
            print "Success!"
    except Exception, err:
            db.rollback()
            print 'MySQL error.'
            print err
            exit(1)

def pinger_mysql_execute(config, title, proto, conn_string):
    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'])
        cur = db.cursor()
        cur.execute("SELECT * FROM remote_stor_ping WHERE connect_str='%s';" % conn_string)
        if int(cur.rowcount) is not 0:
            print "Record for this instance allready exist"
        else:
            cur.execute("insert into remote_stor_ping values ('%s','%s','4','','','%s', NULL, NULL);" % (title, proto, conn_string))
            db.commit()
            print "Success!"
    except Exception, err:
            db.rollback()
            print 'MySQL error.'
            print err
            exit(1)

def add_backup(config_file, type, inst, bull, skip_check, skip_backup, backup_retention=14, machine_retention=1, gzip_period=3):

    insert_tmpl = ("insert into backup.server_backups (host, type, rsync_host, rsync_modulepath, rsync_login, backup_retention, machine_retention, gzip_period, tarantool_snaps_dir, tarantool_xlogs_dir, skip_check, skip_backup, optfile_list) values")
    config = load_config(config_file, 'backup')
    fqdn = (socket.getfqdn())
    short = fqdn.split('.')[0]
    hostname = short + '.i'
    rsync_user = 'my_backup'

    if opts.auto:
        bk_dict = get_tt_json('backup')
        names_list = ['hostname', 'rsync_host', 'type', 'module', 'snaps_dir', 'xlogs_dir', 'backup_retention', 'machine_retention', 'skip_check', 'skip_backup']

        for inst in bk_dict.values():
            bk_type = 'tarantool' if ('octopus' == inst['type'] or 'tarantool' == inst['type']) else 'mysql'
            module = '%s/%s/%s/%s' % (rsync_user, inst['type'], short, inst['title'])
            rsync_host = get_bull()
            data_list = [hostname, rsync_host, bk_type, module, inst['snaps'], inst['xlogs'], backup_retention, machine_retention, skip_check, skip_backup]

            print_insert_data(names_list, data_list, 'backup')
            backup_mysql_execute(config, insert_tmpl, hostname, bk_type, rsync_host, module, rsync_user, backup_retention, machine_retention, gzip_period, inst['snaps'], inst['xlogs'], skip_check, skip_backup)

def add_pinger(config_file):
    config = load_config(config_file, 'pinger')

    if opts.auto:
        ping_dict = get_tt_json('pinger')
        names_list = ['title', 'conn_string', 'type', 'proto']

        for port, inst in ping_dict.items():
            title = '%s-%s:%s' % (inst['title'], inst['ip'], port)
            conn_string = '%s:%s' % (inst['ip'], port)
            type = inst['type']
            proto = inst['proto']
            data_list = [title, conn_string, type, proto]

            print_insert_data(names_list, data_list, 'pinger')
            pinger_mysql_execute(config, title, proto, conn_string)

if opts.auto:
    #add_backup(opts.config, opts.type, 'auto', opts.bull, '0', '0')
    add_pinger(opts.config)
