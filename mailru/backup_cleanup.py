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
from shutil import rmtree

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
parser.add_option("--log", type="str", dest="log_file", default='/var/log/mailru/backup-cleanup.log',
                  help="Path to log file. Default: /var/log/mailru/backup-cleanup.log")
parser.add_option('--log_level', type='choice', action='store', dest='loglevel', default='INFO',
                  choices=['INFO', 'WARNING', 'CRITICAL', 'DEBUG'], help='Log level. Choose from: INFO, WARNING, CRITICAL and DEBUG. Default is INFO')


(opts, args) = parser.parse_args()

### Global
now = time()
backup_time = strftime('%d.%m.%Y_%H%M', localtime())
error_file = '/var/tmp/backup-cleanup.txt'
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
error_log.setLevel(logging.ERROR)
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
    select_tmpl = "select rsync_modulepath,type,backup_retention from backup.server_backups where rsync_host = %s"
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

def oldest_file(root):

    return max([os.path.join(root, f) for f in os.listdir(root) if not f.startswith('.')], key=os.path.getmtime)

def oldest_dir(root):

    return max([os.path.join(root, d) for d in os.listdir(root)], key=os.path.getmtime)

def make_files_dict(retention_dict):

    ### Mysql root directory pattern
    p = re.compile('\d{4}\.\d{2}\.\d{2}$')

    for inst in retention_dict.keys():
        files = []
        root_dir = "%s/%s" % (basedir, retention_dict[inst]['rsync_modulepath'])
        if retention_dict[inst]['type'] == 'mysql' or retention_dict[inst]['type'] == 'psql':
            for file in [x[0] for x in os.walk(root_dir) if p.findall(x[0])]:
                if file not in oldest_file(os.path.dirname(file)):
                    files.append(file)
            retention_dict[inst]['files'] = files
        else:
            for file in [os.path.join(x[0], y) for x in os.walk(root_dir) for y in x[2] if not y.startswith('.') and y not in oldest_file(x[0])]:
                files.append(file)
            retention_dict[inst]['files'] = files

    return retention_dict

def cleanup(retention_dict):

    for inst in retention_dict.values():
        retention_days = 1 if inst['backup_retention'] == 0 else inst['backup_retention']
        for fullpath in inst['files']:
            logger.debug('File: "%s", retention_days: "%s" -- Checking mtime < retention_time: %s < %s, result: %s' %
                         (fullpath, retention_days, os.lstat(fullpath).st_ctime, now - int(retention_days) * 86400, os.lstat(fullpath).st_ctime < now - int(retention_days) * 86400))
            if os.lstat(fullpath).st_ctime < now - int(retention_days) * 86400:
                logger.info('Deleting %s, older than %s days ago' % (fullpath, retention_days))
                try:
                    if inst['type'] == 'mysql' or inst['type'] == 'psql':
                        rmtree(fullpath)
                    else:
                        os.unlink(fullpath)
                except Exception as e:
                    logger.exception(e)

retention_dict = {}
retention_dict_tmp = get_conf(opts.config, hostname)
for bk in retention_dict_tmp:
    name = bk['rsync_modulepath']
    retention_dict[name] = bk
retention_dict = make_files_dict(retention_dict)
cleanup(retention_dict)
logger.info('Finished')
