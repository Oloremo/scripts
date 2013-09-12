#!/usr/bin/env python

from os import statvfs                      # for geting info of partition
from math import ceil                       # for rounding
from sys import exit, stdout, version_info  # for exit code, output func and version check
from optparse import OptionParser           # for usage

### Gotta catch 'em all!
usage = "usage: %prog -t TYPE [-c LIMIT] [-w LIMIT] [-x PARTITION] [-a FS_TYPE]"
parser = OptionParser(usage=usage)
parser.add_option("-c", "--crit", type="int", dest="crit_limit",
                  help="Critical limit. Default: 20% for 'pct' and 5000Mb for 'space'")
parser.add_option("-w", "--warn", type="int", dest="warn_limit", default=False,
                  help="Warning limit. Default: False")
parser.add_option("-x", type="str", action="append", dest="ex_list",
                  help="Exclude list. This partitions won't be cheked.")
parser.add_option("-a", type="str", action="append", dest="fs_type",
                  help="Append fs to check list. By default we check only 'ext2', 'ext3', 'ext4', 'xfs'")
parser.add_option('-t', '--type', type='choice', action='store', dest='type', default='pct',
                 choices=['pct', 'space'], help='Check type. Chose from "pct" and "space"')

(opts, args) = parser.parse_args()
if opts.warn_limit and opts.warn_limit <= opts.crit_limit:
        parser.error("Configuration error. Warning limit is more than Critical limit.")

if opts.type == 'pct':
        if not opts.crit_limit:
                opts.crit_limit = 20
elif opts.type == 'space':
        if not opts.crit_limit:
                opts.crit_limit = 5000

### Assign global variables
fs_type_list = ['ext2', 'ext3', 'ext4', 'xfs']
if opts.fs_type:
    fs_type_list.extend(opts.fs_type)
ex_list = opts.ex_list

### Version check
if version_info[1] >= 6:
    ### Python 2.6
    isEL6 = True
else:
    ### Python 2.4
    isEL6 = False

### Functions

def output(line):
    if isEL6:
        stdout.write(line + "<br>")
        stdout.flush()
    else:
        print line

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """

    for string in list:
        output(string)

def get_all_mounts(fs_type_list, ex_list):

    if ex_list is None:
        ex_list = []
    mpoint = []

    f = list(open("/proc/mounts", "r"))
    for line in f:
        if line.split()[2] in fs_type_list and line.split()[1] not in ex_list:
            mpoint.append(line.split()[1])
    return mpoint

def get_fs_stat(mount):
    dict = {}
    st = statvfs(mount)

    dict['free'] = float(st.f_bavail * st.f_frsize)
    dict['used'] = float((st.f_blocks - st.f_bfree) * st.f_frsize)
    dict['pct_used'] = int(ceil((100 * dict['used']) / (dict['used'] + dict['free'])))

    return dict

def make_mounts_dict(mounts_list):

    mounts_dict = {}
    for mount in mounts_list:
        mounts_dict[mount] = get_fs_stat(mount)

    return mounts_dict

def bytes2mb(num):
    for x in ['KB', 'MB']:
        num /= 1024.0
    return int(num)

def check_space(mounts_dict, crit, warn, check='pct'):

    result_critical = []
    result_warning = []

    if check == 'pct':
        for mount in mounts_dict.keys():
            if mounts_dict[mount]['pct_used'] >= 100 - crit:
                result_critical.append('%s: less than %s%% free (= %s%%)' % (mount, crit, mounts_dict[mount]['pct_used']))
            if warn and mounts_dict[mount]['pct_used'] >= 100 - warn:
                result_warning.append('%s: less than %s%% free (= %s%%)' % (mount, warn, mounts_dict[mount]['pct_used']))

    if check == 'space':
        for mount in mounts_dict.keys():
            if bytes2mb(mounts_dict[mount]['free']) <= crit:
                result_critical.append('%s: less than %sMb free (= %sMb)' % (mount, crit, bytes2mb(mounts_dict[mount]['free'])))
            if warn and bytes2mb(mounts_dict[mount]['free']) <= warn:
                result_warning.append('%s: less than %sMb free (= %sMb)' % (mount, warn, bytes2mb(mounts_dict[mount]['free'])))

### Depending on situation it prints revelant list filled with alert strings
    if result_critical and result_warning:
        print_list(result_critical)
        print_list(result_warning)
        exit(1)
    elif result_critical and not result_warning:
        print_list(result_critical)
        exit(1)
    elif result_warning:
        print_list(result_warning)
        exit(2)

### Work
mounts = get_all_mounts(fs_type_list, ex_list)
mounts_dict = make_mounts_dict(mounts)

if opts.type == 'pct':
    check_space(mounts_dict, opts.crit_limit, opts.warn_limit)
elif opts.type == 'space':
    check_space(mounts_dict, opts.crit_limit, opts.warn_limit, check='space')
