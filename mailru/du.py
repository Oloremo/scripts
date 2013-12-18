#!/usr/bin/env python

from os import statvfs                      # for geting info of partition
from math import ceil                       # for rounding
from sys import exit, stdout, version_info  # for exit code, output func and version check
from os.path import isfile                  # for OS file check
from optparse import OptionParser           # for usage

### Gotta catch 'em all!
usage = "usage: %prog -t TYPE [-c LIMIT] [-w LIMIT] [-x EXCLUDE]"
parser = OptionParser(usage=usage)
parser.add_option("-c", "--crit", type="int", dest="crit_limit",
                  help="Critical limit. Default: 20% for 'pct' and 5000Mb for 'space'")
parser.add_option("-w", "--warn", type="int", dest="warn_limit", default=False,
                  help="Warning limit. Default: False")
parser.add_option("--conf", type="str", dest="config", default="/etc/du-mon.conf",
                  help="Config file for custom partition limits. Default: /etc/du-mon.conf")
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

def get_all_mounts(fs_type_list, ex_list):

    if ex_list is None:
        ex_list = []
    mpoint = []

    f = open_file('/proc/mounts')
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
    dict['pct_free'] = 100 - dict['pct_used']

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

def check_space(mounts_dict, conf_dict, check='pct'):

    result_critical = []
    result_warning = []

    for mount in mounts_dict.keys():
        if check == 'pct' and conf_dict[mount][2] == '%':
            if mounts_dict[mount]['pct_free'] <= conf_dict[mount][1]:
                result_critical.append('%s: less than %s%% free (= %s%%)' % (mount, conf_dict[mount][1], mounts_dict[mount]['pct_used']))
            elif conf_dict[mount][0] and mounts_dict[mount]['pct_free'] <= conf_dict[mount][0]:
                result_warning.append('%s: less than %s%% free (= %s%%)' % (mount, conf_dict[mount][0], mounts_dict[mount]['pct_used']))

        if check == 'space' and conf_dict[mount][2] == 'm':
            if mounts_dict[mount]['free'] <= conf_dict[mount][1]:
                result_critical.append('%s: less than %s%% free (= %s%%)' % (mount, conf_dict[mount][1], mounts_dict[mount]['free']))
            elif conf_dict[mount][0] and mounts_dict[mount]['free'] <= conf_dict[mount][0]:
                result_warning.append('%s: less than %s%% free (= %s%%)' % (mount, conf_dict[mount][0], mounts_dict[mount]['free']))

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

def make_config(config_file, crit, warn, mounts):
    conf_dict = {}

    ### If there is no conf file we use defaults
    if not isfile(opts.config):
        for mount in mounts:
            ### Becouse python 2.4 does'n have ternary operator I have to use this shit(condition and true or false)
            conf_dict[mount] = tuple([int(warn) and int(warn) or False, int(crit), '%'])
        return conf_dict
    try:
        ### Make dict from conf file
        for mount in mounts:
            conf_dict[mount] = tuple([int(warn) and int(warn) or False, int(crit), '%'])
        for line in open_file(opts.config):
            line = line.strip()
            if line and not line.startswith("#"):
                (key, warn, crit, type) = line.split()
                conf_dict[key.rstrip(':')] = tuple([int(warn), int(crit), type])
    except Exception, err:
        if 'IO_ERROR' in err:
            print err
            exit(1)
        else:
            output("Unhandled exeption. Check me.")
            print err
            exit(1)

    for k, v in conf_dict.items():
        if v[2].strip() == '%' and (v[0] and v[0] not in xrange(1, 100) or v[1] not in xrange(1, 100)):
            output('Config error. "%s" partition limits not in range of 1-100. Config: %s' % (k, config_file))
            exit(1)
        elif v[2].lower().strip() == 'm' and (v[0] < 1 or v[1] < 1):
            output('Config error. "%s" partition limits is less than 1Mb. Sounds wierd... Config: %s' % (k, config_file))
            exit(1)

    return conf_dict

### Work
mounts = get_all_mounts(fs_type_list, ex_list)
mounts_dict = make_mounts_dict(mounts)
conf_dict = make_config(opts.config, opts.crit_limit, opts.warn_limit, mounts)

if opts.type == 'pct':
    check_space(mounts_dict, conf_dict)
elif opts.type == 'space':
    check_space(mounts_dict, conf_dict, check='space')
