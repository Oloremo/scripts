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

if not opts.type == 'auto' and not opts.bull and opts.batch:
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
        print "Unhandled exeption. Check me."
        print err
        exit(1)

#def print_inst_data(short, bk_type, bull, module, backup_retention, retension, snaps_dir, xlogs_dir, skip_check, skip_backup):
#    print """You're about to add this data to backup:
#
#host:                %s
#type:                %s
#rsync_host:          %s
#rsync_modulepath:    %s
#backup_retention:    %s
#machine_retention:   %s
#tarantool_snaps_dir: %s
#tarantool_xlogs_dir: %s
#skip_check:          %s
#skip_backup:         %s
#""" % (short, bk_type, bull, module, backup_retention, retension, snaps_dir, xlogs_dir, skip_check, skip_backup)

def print_inst_data(inst_tuple, type):
    print inst_tuple

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
        return random.choice(top5_bulls)
    elif five_plus:
        top5_bulls = sorted(ten_plus, key=ten_plus.get, reverse=True)[:5]
        return random.choice(top5_bulls)
    elif one_plus:
        top_bull = max(one_plus, key=one_plus.get)
        return top_bull
    else:
        return False

def backup_mysql_execute(config, insert_tmpl, hostname, bk_type, bull, module, rsync_user, rsync_pass, backup_retention, retension, gzip_period, snaps_dir, xlogs_dir, skip_check, skip_backup):
    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'])
        cur = db.cursor()
        ### Check if it's allready exist
        cur.execute("select * from server_backups where host = '%s' and tarantool_snaps_dir='%s' and tarantool_xlogs_dir='%s'" % (hostname, snaps_dir, xlogs_dir))
        if int(cur.rowcount) is not 0:
            print "Record for this instance allready exist"
        else:
            cur.execute("%s ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '');" % (insert_tmpl, hostname, bk_type, bull, module, rsync_user, rsync_pass, backup_retention, retension, gzip_period, snaps_dir, xlogs_dir, skip_check, skip_backup))
            db.commit()
    except Exception, err:
            db.rollback()
            print 'MySQL error.'
            print err
            exit(1)

def pinger_mysql_execute(config, ping_dict):
    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'])
        cur = db.cursor()
        for port in ping_dict.keys():
            cur.execute("SELECT * FROM remote_stor_ping WHERE connect_str='%s:%s';" % (ping_dict[port]['ip'], port))
            if int(cur.rowcount) is not 0:
                print "Record for this instance allready exist"
            else:
                cur.execute("insert into remote_stor_ping values ('%s-%s:%s','iproto','4','','','%s:%s', NULL, NULL);" % (ping_dict[port]['title'], ping_dict[port]['ip'], port, ping_dict[port]['ip'], port))
                db.commit()
    except Exception, err:
            db.rollback()
            print 'MySQL error.'
            print err
            exit(1)

def add_backup(config_file, type, inst, bull, skip_check, skip_backup, backup_retention=14, retension=1, gzip_period=3):

    insert_tmpl = ("insert into backup.server_backups (host, type, rsync_host, rsync_modulepath, rsync_login, rsync_passwd, backup_retention, machine_retention, gzip_period, tarantool_snaps_dir, tarantool_xlogs_dir, skip_check, skip_backup, optfile_list) values")
    config = load_config(config_file, 'backup')
    fqdn = (socket.getfqdn())
    short = fqdn.split('.')[0]
    hostname = short + '.i'
    rsync_user = 'my_backup'
    rsync_pass = 'reemaNg5hahku3ho'

    if opts.auto:
        bk_dict = get_tt_json('backup')

        for inst in bk_dict.values():
            type = inst['type']
            bk_type = 'tarantool' if ('octopus' == type or 'tarantool' == type) else 'mysql'
            title = inst['title']
            snaps_dir = inst['snaps']
            xlogs_dir = inst['xlogs']
            module = '%s/%s/%s/%s' % (rsync_user, type, short, title)
            bull = get_bull()
            print bull

            #print_inst_data(short, bk_type, bull, module, backup_retention, retension, snaps_dir, xlogs_dir, skip_check, skip_backup)
            if not opts.batch:
                yes_no()

            backup_mysql_execute(config, insert_tmpl, hostname, bk_type, bull, module, rsync_user, rsync_pass, backup_retention, retension, gzip_period, snaps_dir, xlogs_dir, skip_check, skip_backup)
    else:
        snaps_dir = '/var/%s%s/snaps' % (type, inst)
        xlogs_dir = '/var/%s%s/xlogs' % (type, inst)
        bk_type = 'tarantool' if ('octopus' == type or 'tarantool' == type) else 'mysql'
        module = '%s/%s/%s/%s' % (rsync_user, type, short, type)

def add_pinger(config_file):
    config = load_config(config_file, 'pinger')
    data_names = ('title', 'ip', 'port')
    data_list = []

    if opts.auto:
        ping_dict = get_tt_json('pinger')
        #for port in ping_dict.keys():
        #    ping_dict[port]['port'] = port
        #for inst in ping_dict.values():
        #    for name in data_names:
        #        data_list.append((name, inst[name]))
        #print data_list
        #print_inst_data(ping_dict, 'pinger')

        if not opts.batch:
            yes_no()

        pinger_mysql_execute(config, ping_dict)


if opts.auto:
    add_backup(opts.config, opts.type, 'auto', opts.bull, '0', '0')
    add_pinger(opts.config)
