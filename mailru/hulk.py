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
from sys import exit, stdout
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

def output(line):
    stdout.write(str(line) + "<br>")
    stdout.flush()

def print_timestamp():
    return strftime('%d %b %Y %H:%M:%S', localtime())

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

def get_conf(config_file, type, hostname):
    """ Get backup configuration from dracula database """

    config = load_config(config_file, 'backup')
    select_tmpl = "select * from backup.server_backups where host = %s and type= %s"
    select_data = (hostname, type)

    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'], cursorclass=MySQLdb.cursors.DictCursor)
        cur = db.cursor()
        cur.execute(select_tmpl, select_data)
        if int(cur.rowcount) is 0:
            print "Cant find any records in dracula db for hostname '%s' and type '%s'" % (hostname, type)
        else:
            return cur.fetchall()
    except Exception, err:
            output('MySQL error. Check me.')
            print err
            ### We cant print exeption error here 'cos it can contain auth data
            exit(1)

def write_to_err_file(type, error):

    filename = '/var/tmp/hulk-%s.txt' % type
    with open(filename, 'w') as error_file:
        error_file.write('%s - %s\n' % (print_timestamp(), error))

def create_rsync_dirs(inst, hostname, type):

    rsync_root = '/tmp/rsync_tmpl/'
    inst_name = inst['base_dir'].split('/')[-1]
    fullpath = '%s%s/%s/%s/%s' % (rsync_root, inst['type'], hostname, inst_name, type)
    print "Creating backup dir structure: %s" % fullpath

    rmtree(rsync_root, ignore_errors=True)

    if not os.path.exists(fullpath):
        os.makedirs(fullpath)

    rsync_files(rsync_root, False, False, inst['rsync_opts'], inst['rsync_host'], inst['rsync_login'], '', inst['rsync_passwd'], inst['type'])

def rsync_files(root_dir, files, exclude, rsync_args, host, module, module_path, rsync_pass, type):

    rsync = '/usr/bin/rsync'
    rsync_host = '%s@%s::%s/%s' % (module, host, module, module_path)
    if files:
        rsync_files = ''
        for file in files:
            rsync_files += file + ' '
        rsync_run = '%s %s %s %s' % (rsync, rsync_args, rsync_files, rsync_host)
    else:
        rsync_run = '%s %s %s %s' % (rsync, rsync_args, root_dir, rsync_host)
    my_env = os.environ.copy()
    my_env['RSYNC_PASSWORD'] = rsync_pass

    ### FIXME
    print "Executing: %s" % rsync_run
    rsync_output = subprocess.Popen(rsync_run, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, env=my_env).communicate()
    #print rsync_output
    if rsync_output[1]:
        write_to_err_file(type, rsync_output[1])

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

    print "Copying %s to %s" % (path_to_file, tmp_fullpath)
    file = open(path_to_file, 'r')
    if lock_file(file, timeout):
        copyfile(path_to_file, tmp_fullpath)
        fcntl.flock(file, fcntl.LOCK_UN)
        file.close()
    else:
        print "Cant' acquire lock for %s seconds, giving up!" % timeout
        file.close()

def make_tarfile(output_filename, source_dir):
    tar = tarfile.open(output_filename, "w")
    tar.add(source_dir, arcname=os.path.basename(source_dir))

def clenup(inst, type):
    oldest_snap = sorted(os.listdir(inst['base_dir'] + '/snaps'))[-1]
    oldest_snap_lsn = int(oldest_snap.split('.')[0])
    limit = inst['machine_retention'] if int(inst['machine_retention']) > 0 else 1
    limit_ut = limit * 86400

    if type == 'snaps':
        base_dir = inst['base_dir'] + '/' + type
        for file in [file for file in os.listdir(base_dir) if file != oldest_snap]:
            fullpath = base_dir + '/' + file
            if os.lstat(fullpath).st_mtime < now - limit_ut:
                print 'Deleting %s, older than %s days' % (fullpath, limit)
                os.unlink(fullpath)
    if type == 'xlogs':
        base_dir = inst['base_dir'] + '/' + type
        for file in [file for file in os.listdir(base_dir) if int(file.split('.')[0]) < oldest_snap_lsn]:
            fullpath = base_dir + '/' + file
            if os.lstat(fullpath).st_mtime < now - limit_ut:
                print 'Deleting %s, older than %s days' % (fullpath, limit)
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
                tar_name = '%s/walroot%s_%s.tar' % (tmpdir, xdata, backup_time)
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
                print "Making tar at %s" % xdata_base_dir
                make_tarfile(tar_name, xdata_base_dir + '/')
                tars[xdata].append(tar_name)

            for xdata in tars.keys():
                tmpdir = global_tmpdir if global_tmpdir else base_dir + '/' + xdata + '/backup_temp'
                rsync_files(base_dir, tars[xdata], False, inst['rsync_opts'], inst['rsync_host'], inst['rsync_login'], module_path, inst['rsync_passwd'], inst['type'])
                print "Deleting temp dir: %s" % tmpdir
                rmtree(tmpdir)

        elif type == 'snaps' or type == 'xlogs':
            create_rsync_dirs(inst, hostname, type)
            backupdir = '%s/%s/' % (inst['base_dir'], type)
            rsync_files(backupdir, False, False, inst['rsync_opts'], inst['rsync_host'], inst['rsync_login'], module_path, inst['rsync_passwd'], inst['type'])
            clenup(inst, type)

        elif type == 'other':
            create_rsync_dirs(inst, hostname, type)
            files = inst['optfile_list'].split(',')
            rsync_files(False, files, False, inst['rsync_opts'], inst['rsync_host'], inst['rsync_login'], module_path, inst['rsync_passwd'], inst['type'])

### Hostname
fqdn = (socket.getfqdn())
short = fqdn.split('.')[0]
hostname = short + '.i'

inst_dict = get_conf(opts.config, 'silver', hostname)
backup(inst_dict, opts.mode, short, opts.tmpdir, opts.timeout)
