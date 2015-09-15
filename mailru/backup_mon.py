#!/usr/bin/env python

import socket
import MySQLdb
import MySQLdb.cursors
import simplejson as json
import os
import re
import logging
from sys import exit
from os.path import isfile
from optparse import OptionParser
from time import time, localtime, strftime

### Gotta catch 'em all!
usage = "usage: %prog "

parser = OptionParser(usage=usage)
parser.add_option('-t', '--type', type='choice', action='store', dest='type',
                  choices=['mysql', 'tt', 'silver'],
                  help='Backup type. Chose from "mysql", "tt" and "silver"')
parser.add_option('-m', '--mode', type='choice', action='store', dest='mode',
                  choices=['snaps', 'xlogs', 'xdata', 'other'],
                  help='Backup mode. Chose from "xlogs", "snaps", "xdata" and "other"')
parser.add_option("--conf", dest="config", type="str", default="/etc/hal9000.conf",
                  help="Config file. Default: /etc/hal9000.conf")
parser.add_option("--tmpdir", dest="tmpdir", type="str", default="",
                  help="Config file. Default: /etc/hal9000.conf")
parser.add_option("--timeout", dest="timeout", type="int", default=5,
                  help="File lock timeout Default: 5")
parser.add_option("--log", type="str", dest="log_file", default='/var/log/mailru/backup-mon.log',
                  help="Path to log file. Default: /var/log/mailru/backup-mon.log")
parser.add_option('--log_level', type='choice', action='store', dest='loglevel', default='INFO',
                  choices=['INFO', 'WARNING', 'CRITICAL', 'DEBUG'], help='Log level. Choose from: INFO, WARNING, CRITICAL and DEBUG. Default is INFO')


(opts, args) = parser.parse_args()

### Global
now = time()
backup_time = strftime('%d.%m.%Y_%H%M', localtime())
error_file = '/var/tmp/backup-mon.txt'
loglevel = logging.getLevelName(opts.loglevel)
basedir = '/backup'

### Hostname
fqdn = (socket.getfqdn())
short = fqdn.split('.')[0]
hostname = short + '.i'

###Logger init

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

error_log = logging.FileHandler(error_file, mode='w')
error_log.setLevel(logging.CRITICAL)
format = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
eformat = logging.Formatter('%(asctime)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
error_log.setFormatter(eformat)
logger.addHandler(error_log)

if not os.path.exists("/var/log/mailru"):
    logger.critical('Path "/var/log/mailru" is not exists - unable to start.')
    exit(1)
else:
    mainlog = logging.FileHandler(opts.log_file)
    mainlog.setLevel(loglevel)
    mainlog.setFormatter(format)
    logger.addHandler(mainlog)

logger.info('=====================================================================================================')
logger.info('Started')

def load_config(file, type):
    logger.info('Loading config file "%s"' % file)
    if not isfile(file):
        logger.critical('Config load error. File %s not found.' % file)
        exit(1)
    try:
        config = json.load(open(file))
        if type in config:
            return config[type]
        else:
            logger.critical('Cant load "%s" key from config %s' % (type, file))
            exit(2)
    except Exception:
        logger.exception('Unhandled exeption. Check me.')
        exit(1)

def get_conf(config_file, hostname):
    """ Get backup configuration from dracula database """

    logger.info('Loading config for this host from dracula')
    config = load_config(config_file, 'backup')
    select_tmpl = "select * from backup.server_backups where rsync_host = %s and skip_backup = 0"
    select_data = (hostname)

    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'], cursorclass=MySQLdb.cursors.DictCursor, connect_timeout=1, read_timeout=1)
        cur = db.cursor()
        cur.execute(select_tmpl, select_data)
        if int(cur.rowcount) is 0:
            logger.warning("Can't find any records in dracula db for hostname '%s' and type '%s'" % (hostname, type))
            exit(1)
        else:
            return cur.fetchall()
    except Exception:
            logger.exception('MySQL error. Check me.')
            exit(1)

def make_files_dict(retention_dict):

    ### Mysql root directory pattern
    p = re.compile('\d{4}\.\d{2}\.\d{2}$')

    for inst in retention_dict.keys():
        files = []
        root_dir = "%s/%s" % (basedir, retention_dict[inst]['rsync_modulepath'])
        if retention_dict[inst]['type'] == 'mysql' or retention_dict[inst]['type'] == 'psql':
            for file in [x[0] for x in os.walk(root_dir)]:
                if p.findall(file):
                    files.append(file)
            retention_dict[inst]['files'] = files
        else:
            for file in [os.path.join(x[0], y) for x in os.walk(root_dir) for y in x[2]]:
                files.append(file)
            retention_dict[inst]['files'] = files

    return retention_dict

def get_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def check_dir(inst, type, mode):
    if type == 'xlogs':
        limit_ut = int(inst['xlogs_mon_max_lag']) * 3600
    elif type == 'snaps':
        limit_ut = int(inst['snaps_mon_max_lag']) * 3600
    elif type == 'other':
        limit_ut = int(inst['other_mon_max_lag']) * 3600
    else:
        limit_ut = 86400

    if mode == 'tarantool':
        root = '/backup/%s/%s/' % (inst['rsync_modulepath'], type)
    elif mode == 'silver':
        name = inst['base_dir'].rsplit('/')[-1]
        root = '/backup/%s/%s/%s/' % (inst['rsync_modulepath'], name, type)

    if not os.path.isdir(root):
        logger.critical("Directory '%s' does not exist" % root)
        return
    if os.listdir(root):
        ### "Other" backups tend to have same mtime for a long period of time, so we will check their ctime
        if type == 'other':
            oldest_file = max([os.path.join(root, f) for f in os.listdir(root) if not f.startswith('.')], key=os.path.getctime)
            if os.lstat(oldest_file).st_ctime < now - limit_ut:
                hours_ago = int((now - os.lstat(oldest_file).st_ctime) / 60 // 60)
                logger.critical("Last backup in '%s' was made more than %s hours ago" % (root, hours_ago))
        else:
            oldest_file = max([os.path.join(root, f) for f in os.listdir(root) if not f.startswith('.')], key=os.path.getmtime)
            if os.lstat(oldest_file).st_mtime < now - limit_ut:
                hours_ago = int((now - os.lstat(oldest_file).st_mtime) / 60 // 60)
                logger.critical("Last backup in '%s' was made more than %s hours ago" % (root, hours_ago))
    else:
        logger.critical("Directory '%s' is empty" % root)

def check_mysql(inst):

    name = inst['host'].rstrip('.i')
    limit_ut = 86400
    root = '/backup/' + inst['rsync_modulepath'] + '/' + name
    last_bk = max([os.path.join(root, f) for f in os.listdir(root)], key=os.path.getmtime)
    if os.lstat(last_bk).st_mtime < now - limit_ut:
            hours_ago = int((now - os.lstat(last_bk).st_mtime) / 60 // 60)
            logger.critical("Last backup in '%s' was made more than %s hours ago" % (root, hours_ago))
    if inst['min_size'] != 0:
        last_bk_size = get_size(last_bk) / 1024 // 1024
        if last_bk_size < inst['min_size']:
            logger.critical("Current size of %s is %sMb. Expected more than %sMb." % (last_bk, last_bk_size, inst['min_size']))

def check(retention_dict):

    for inst in sorted(retention_dict):
        if inst['type'] == 'tarantool':
            if inst['tarantool_snaps_dir']:
                check_dir(inst, 'snaps', 'tarantool')
            if inst['tarantool_xlogs_dir']:
                check_dir(inst, 'xlogs', 'tarantool')
        if inst['type'] == 'silver':
            if inst['base_dir']:
                check_dir(inst, 'xdata', 'silver')
                check_dir(inst, 'snaps', 'silver')
                check_dir(inst, 'xlogs', 'silver')
            if inst['base_dir'] and inst['optfile_list']:
                check_dir(inst, 'other', 'silver')
        if inst['type'] == 'mysql':
            check_mysql(inst)

retention_dict = {}
retention_dict = get_conf(opts.config, hostname)

check(retention_dict)
logger.info('Finished')
