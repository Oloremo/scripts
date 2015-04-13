#!/usr/bin/env python

import os
import simplejson as json
from os.path import isfile
from time import time, localtime, strftime

now = time()

def print_timestamp():
    return strftime('%d %b %Y %H:%M:%S', localtime())

def load_config(file):
    if not isfile(file):
        print "Config load error. File %s not found." % file
        exit(1)
    try:
        config = json.load(open(file))
        return config
    except Exception, err:
        print "Error while load config from %s. Unhandled exeption. Check me." % file
        print err
        exit(1)

def limit_to_unixtime(limit):
    if limit[1] == 'days':
        return int(limit[0]) * 86400
    elif limit[1] == 'mins':
        return int(limit[0]) * 60
    else:
        return False

def is_dir_in_limits(dir, limits_dict):
    for key in limits_dict.keys():
        return limits_dict[key] if key in dir else limits_dict['DEFAULT']

def cleanup(dirname, filenames, limit):
    for file in filenames:
        fullpath = dirname + '/' + file
        if limit_to_unixtime(limit):
            if os.stat(fullpath).st_mtime < now - limit_to_unixtime(limit):
                print 'Deleting %s, older than %s' % (fullpath, limit)
                os.remove(fullpath)
        else:
            print "Limits type for %s is not 'days' or 'mins' - cant do shit. Fix it!" % file
            continue

print "%s - Starting... " % print_timestamp()
print '%s - Loading config...' % print_timestamp()
limits = load_config('/usr/local/etc/logs-cleanup.conf')

scribe_root = '/logs/scribe/'
nxlog_root = '/logs/nxlog/'
print '%s - Making files index for nxlog...' % print_timestamp()
nxlog_dirs = [nxlog_root + dir for dir in os.listdir(nxlog_root)]
print '%s - Making files index for scribe...' % print_timestamp()
scribe_dirs = [scribe_root + dir for dir in os.listdir(scribe_root)]

logsdir_list = nxlog_dirs + scribe_dirs

for dir in logsdir_list:
    limit = is_dir_in_limits(dir, limits)
    print '==================================='
    print '%s - Working at %s' % (print_timestamp(), dir)
    for dirname, dirnames, filenames in os.walk(dir):
        cleanup(dirname, filenames, limit)
    if not os.listdir(dir):
        print "Directory %s is empty - deleting" % dir
        os.rmdir(dir)
