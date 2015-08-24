#!/usr/bin/env python

import socket
import subprocess
import MySQLdb
import MySQLdb.cursors
import simplejson as json
import os
import fcntl
import errno
import tarfile
import logging
from sys import exit
from os.path import isfile, isdir
from optparse import OptionParser
from time import time, localtime, strftime, sleep
from shutil import rmtree, copyfile

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
parser.add_option("--log", type="str", dest="log_file",
                  help="Path to log file. Default: /var/log/mailru/hulk-$type-$mode.log")
parser.add_option('--log_level', type='choice', action='store', dest='loglevel', default='INFO',
                  choices=['INFO', 'WARNING', 'CRITICAL', 'DEBUG'], help='Log level. Choose from: INFO, WARNING, CRITICAL and DEBUG. Default is INFO')


(opts, args) = parser.parse_args()

if not opts.type:
    print "Choose type of backup"
    parser.print_help()
if not opts.mode:
    print "Choose mode of backup"
    parser.print_help()

### Global
now = time()
backup_time = strftime('%d.%m.%Y_%H%M', localtime())
error_file = '/var/tmp/hulk-'
loglevel = logging.getLevelName(opts.loglevel)


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

def get_conf(config_file, type, hostname):
    """ Get backup configuration from dracula database """

    logger.info('Loading config for this host from dracula')
    config = load_config(config_file, 'backup')
    select_tmpl = "select * from backup.server_backups where host = %s and type= %s"
    select_data = (hostname, type)

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

def create_rsync_dirs(inst, hostname, type):
    """ We create needed directory structure and rsync it to bull """

    rsync_root = '/tmp/rsync_tmpl/%s/' % type
    inst_name = inst['base_dir'].split('/')[-1]
    fullpath = '%s/%s/%s/%s/%s' % (rsync_root, inst['type'], hostname, inst_name, type)
    logger.info("Creating backup dir structure: %s" % fullpath)

    rmtree(rsync_root, ignore_errors=True)

    if not os.path.exists(fullpath):
        os.makedirs(fullpath)

    rsync_files(rsync_root, False, False, inst['rsync_opts'], inst['rsync_host'], inst['rsync_login'], '', inst['rsync_passwd'], inst['type'])

def rsync_files(root_dir, files, exclude, rsync_args, host, module, module_path, rsync_pass, type, add_timestamp=False):

    rsync = '/usr/bin/rsync'
    if module_path:
        rsync_host = '%s@%s::%s/%s/' % (module, host, module, module_path)
    else:
        rsync_host = '%s@%s::%s/' % (module, host, module)

    my_env = os.environ.copy()
    my_env['RSYNC_PASSWORD'] = rsync_pass

    if files:
        for file in files:
            if add_timestamp:
                rsync_host += '%s.%s' % (file.split('/')[-1], backup_time)
                cmdline = '%s %s %s %s' % (rsync, rsync_args, file, rsync_host)
                rsync_run(cmdline, my_env)
            else:
                cmdline = '%s %s %s %s' % (rsync, rsync_args, rsync_files, rsync_host)
                rsync_run(cmdline, my_env)
    else:
        cmdline = '%s %s %s %s' % (rsync, rsync_args, root_dir, rsync_host)
        rsync_run(cmdline, my_env)

def rsync_run(cmdline, env):

    sp = subprocess.Popen(cmdline, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, env=env)

    ### FIXME
    logger.info("Executing: %s" % cmdline)
    rsync_output = sp.communicate()
    exitcode = sp.returncode
    logger.debug("Exit code is: %i" % exitcode)
    if rsync_output[0] and exitcode is 0:
        for line in rsync_output[0].splitlines():
            logger.debug("%s" % line)
    if exitcode != 0:
        logger.critical("Rsync return error:")
        for line in rsync_output[0].splitlines():
            logger.critical("%s" % line)
    if rsync_output[1]:
        logger.critical("Rsync return error: %s" % rsync_output[1])

def lock_file(file, timeout):

    start = time()
    while True:
        try:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as e:
            # raise on unrelated IOErrors
            if e.errno == errno.EAGAIN:
                if time() - start > timeout:
                    return False
                sleep(0.1)
            else:
                raise

def copy_file(path_to_file, tmp_fullpath, timeout):

    logger.debug("Copying %s to %s" % (path_to_file, tmp_fullpath))
    file = open(path_to_file, 'r')
    if lock_file(file, timeout):
        copyfile(path_to_file, tmp_fullpath)
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()
    else:
        logger.warning("Cant' acquire lock for %s seconds, giving up!" % timeout)
        logger.critical("Cant' acquire lock for %s seconds, giving up!" % timeout)
        file.close()

def make_tarfile(output_filename, source_dir):
    tar = tarfile.open(output_filename, "w")
    tar.add(source_dir, arcname=os.path.basename(source_dir))

def cleanup(inst, type):
    oldest_snap = sorted(os.listdir(inst['base_dir'] + '/snaps'))[-1] if os.listdir(inst['base_dir'] + '/xlogs') else ''
    oldest_xlog = sorted(os.listdir(inst['base_dir'] + '/xlogs'))[-1] if os.listdir(inst['base_dir'] + '/xlogs') else ''
    oldest_snap_lsn = int(oldest_snap.split('.')[0]) if oldest_snap else 0
    limit = inst['machine_retention'] if int(inst['machine_retention']) > 0 else 1
    limit_ut = limit * 86400

    if type == 'snaps':
        base_dir = inst['base_dir'] + '/' + type
        for file in [file for file in os.listdir(base_dir) if file != oldest_snap]:
            fullpath = base_dir + '/' + file
            if os.lstat(fullpath).st_mtime < now - limit_ut:
                logger.info('Deleting %s, older than %s days' % (fullpath, limit))
                os.unlink(fullpath)
    if type == 'xlogs':
        base_dir = inst['base_dir'] + '/' + type
        for file in [file for file in os.listdir(base_dir) if int(file.split('.')[0]) < oldest_snap_lsn and file != oldest_xlog]:
            fullpath = base_dir + '/' + file
            if os.lstat(fullpath).st_mtime < now - limit_ut:
                logger.info('Deleting %s, older than %s days' % (fullpath, limit))
                os.unlink(fullpath)

def backup(inst_dict, type, hostname, global_tmpdir, timeout):

    for inst in inst_dict:
        inst_name = inst['base_dir'].split('/')[-1]
        module_path = '%s/%s/%s/%s' % (inst['type'], hostname, inst_name, type)

        if type == 'xdata' and inst['type'] == 'silver':
            base_dir = inst['base_dir'] + '/xdata'
            xdata_dirs = [file for file in os.listdir(base_dir) if isdir(base_dir + '/' + file)]
            tars = {}
            create_rsync_dirs(inst, hostname, type)

            for xdata in xdata_dirs:
                tars[xdata] = []
                xdata_base_dir = base_dir + '/' + xdata
                tmpdir = global_tmpdir if global_tmpdir else xdata_base_dir + '/backup_temp'
                tar_name = '%s/walroot%s.tar' % (tmpdir, xdata)
                if not os.path.exists(tmpdir):
                    os.makedirs(tmpdir)
                else:
                    ### FIXME
                    rmtree(tmpdir)
                    os.makedirs(tmpdir)
                for root, dirs, files in os.walk(xdata_base_dir, followlinks=True):
                    if 'backup_temp' not in root and xdata_base_dir != root:
                        os.makedirs(tmpdir + root)
                    if root and 'backup_temp' not in root and xdata_base_dir != root:
                        for file in files:
                            fullpath = root + '/' + file
                            tmp_fullpath = tmpdir + root + '/' + file
                            copy_file(fullpath, tmp_fullpath, timeout)
                logger.info("Making tar for %s as %s" % (xdata_base_dir, tar_name))
                make_tarfile(tar_name, tmpdir)
                tars[xdata].append(tar_name)

            for xdata in tars.keys():
                tmpdir = global_tmpdir if global_tmpdir else base_dir + '/' + xdata + '/backup_temp'
                rsync_files(base_dir, tars[xdata], False, inst['rsync_opts'], inst['rsync_host'], inst['rsync_login'], module_path, inst['rsync_passwd'], inst['type'], add_timestamp=True)
                logger.info("Deleting temp dir: %s" % tmpdir)
                rmtree(tmpdir)

        elif type == 'snaps' or type == 'xlogs':
            create_rsync_dirs(inst, hostname, type)
            backupdir = '%s/%s/' % (inst['base_dir'], type)
            rsync_files(backupdir, False, False, inst['rsync_opts'], inst['rsync_host'], inst['rsync_login'], module_path, inst['rsync_passwd'], inst['type'])
            cleanup(inst, type)

        elif type == 'other' and inst['optfile_list'] != '':
            create_rsync_dirs(inst, hostname, type)
            files = inst['optfile_list'].split(',')
            rsync_files(False, files, False, inst['rsync_opts'], inst['rsync_host'], inst['rsync_login'], module_path, inst['rsync_passwd'], inst['type'], add_timestamp=True)

###Logger init

logname = "%s %s" % (opts.type.upper(), opts.mode.upper())
logger = logging.getLogger(logname)
logger.setLevel(logging.DEBUG)

error_log = logging.FileHandler(error_file + opts.type + '.txt', mode='a')
error_log.setLevel(logging.CRITICAL)
format = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
eformat = logging.Formatter('%(asctime)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
error_log.setFormatter(eformat)
logger.addHandler(error_log)

if not os.path.exists("/var/log/mailru"):
    logger.critical('Path "/var/log/mailru" is not exists - unable to start.')
    exit(1)
else:
    if not opts.log_file:
        logfile = '/var/log/mailru/hulk-%s-%s.log' % (opts.type, opts.mode)
        mainlog = logging.FileHandler(logfile)
    else:
        mainlog = logging.FileHandler(opts.log_file)
    mainlog.setLevel(loglevel)
    mainlog.setFormatter(format)
    logger.addHandler(mainlog)

logger.info('=====================================================================================================')
logger.info('Started')

### Hostname
fqdn = (socket.getfqdn())
short = fqdn.split('.')[0]
hostname = short + '.i'

inst_dict = get_conf(opts.config, 'silver', hostname)
backup(inst_dict, opts.mode, short, opts.tmpdir, opts.timeout)
logger.info("Finished")
