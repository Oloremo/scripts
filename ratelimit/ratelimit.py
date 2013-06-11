#!/usr/bin/env python

from sys import exit, stdout              # for exit codes
from datetime import datetime, timedelta  # for getting a date
from time import strptime                 # for time convertation
from optparse import OptionParser         # for usage
from os.path import isfile                # for OS file check

### Gotta catch 'em all!
usage = "usage: %prog [-f /path/to/file] [--dict] [-l /path/to/dict] [-c critical_limit] [-w warning_limit] [-d delta_in_minutes]"
parser = OptionParser(usage=usage)
parser.add_option("-f", "--file", dest="error_file", default="/var/tmp/error.txt",
                  help="Path to file to check. Default: /var/tmp/error.txt")
parser.add_option("--dict", action="store_true", dest="use_dict", default=False,
                  help="Turn on custom limits taken fron onlineconf. Boolean. Default: False")
parser.add_option("-l", "--limits", dest="dict_file", default="/usr/local/etc/onlineconf/monitoring.conf",
                  help="Path to file to file with custom limits. Default: /usr/local/etc/onlineconf/monitoring.conf")
parser.add_option("-c", "--crit", type="int", dest="critical", default=5,
                  help="Critical limit. Default: 5")
parser.add_option("-w", "--warn", type="int", dest="warning", default=3,
                  help="Warning limit. Default: 3")
parser.add_option("-d", "--delta", type="int", dest="delta", default=60,
                  help="Delta in minutes. Default: 60")
parser.add_option("-s", "--string", dest="regexp", default="restarted with exit code",
                  help="May be we'll need to change it someday...")

(options, args) = parser.parse_args()
if options.warning >= options.critical:
        parser.error("Configuration error. Warning limit is more than Critical limit.")

### Assign global variables
error_file = options.error_file
dict_file = options.dict_file
regexp = options.regexp

### Because of python 2.4 didn't have datetime.strptime we use this shit
if hasattr(datetime, 'strptime'):
    ### Python 2.6+
    strptime_loc = datetime.strptime
    isEL6 = True
else:
    ### Python 2.4 equivalent
    strptime_loc = lambda date_string, format: datetime(*(strptime(date_string, format)[0:6]))

### Stop! Function time!

def output(line):
    if isEL6:
        stdout.write(line + "<br>")
        stdout.flush()
    else:
        print line

def open_file(filename):
    """ We try to open file and copy it into list. """
    if not isfile(filename):
        output("I/O error. There is no '%s'. Check me." % filename)
        raise Exception('NO_FILE')
    try:
        return list(open(filename))
    except IOError, error:
        output("I/O error. Can't open file '%s'. Check me." % filename)
        output("I/O error({0}): {1}".format(error.errno, error.strerror))
        raise Exception('IO_ERROR')
    except:
        raise Exception

def search_not_wrapped(list):
    """ Looking through list and look for non wrapped lines """
    wrong_lines = []
    for string in list:
        if string.strip() and not regexp in string:
            wrong_lines.append(string)
    if len(wrong_lines) != 0:
        output("Input error. There is %s non wrapped lines inside %s" % (len(wrong_lines), error_file))
        if len(wrong_lines) <= 10:
            print_list(wrong_lines)
            return True
        else:
            output("This is first 10:")
            for num in range(10):
                output(wrong_lines[num])
            return True
    else:
        return False

def get_uniq_daemons(list):
    """ Sorting all demons and return list of uniq daemons """
    uniq_daemons = []
    for string in list:
        if string.strip():
            words = string.split()
            uniq_daemons.append(words[2])
    return sorted(set(uniq_daemons))

def compare_timestamp(timestamp, delta):
    """ Subtract daemons crush timestamp from current time and compare result with delta """
    date_now = datetime.now()
    timestamp = strptime_loc(timestamp, "%Y.%m.%d-%H.%M")
    if date_now - timestamp <= timedelta(minutes=delta):
        return True
    else:
        return False

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """
    for string in list:
        output(string)

def set_limits(list, daemon, def_critical, def_warning, def_delta):
    """ We check daemons name againt dictonary and set limits acording to it """

    ### We don't care about instances here so we strip all digits, dots and whitespaces
    daemon = daemon.rstrip('0123456789. ')
    for line in list:
        if line.startswith('daemons-restart'):
            if daemon in line:
                ### Split to name[0] and limits[1]
                line = line.split()
                ### Split limits[1] into 3 limits
                line = line[1].split(',')
                limits = {'crit': line[0], 'warn': line[1], 'delta': line[2]}
                for key, value in limits.iteritems():
                    if value == "0":
                        if key == 'crit':
                            limits[key] = def_critical
                        elif key == 'warn':
                            limits[key] = def_warning
                        elif key == 'delta':
                            limits[key] = def_delta
                return limits
    ### If we didn't find daemon in dictonary, we return default limits
    limits = {'crit': def_critical, 'warn': def_warning, 'delta': def_delta}
    return limits

def check_delta(daemon, list, delta):
    daemons_dict[daemon] = 0
    for string in list:
        if daemon in string:
            string = string.split()
            timestamp = string[0]
            if compare_timestamp(timestamp, delta):
                daemons_dict[daemon] += 1
    return daemons_dict

### Check existence of a file, then copy file into list.
try:
    error_file_list = open_file(error_file)
    if options.use_dict:
        dict_file_list = open_file(dict_file)
except Exception, err:
    if 'NO_FILE' in err:
        exit(3)
    elif 'IO_ERROR' in err:
        exit(1)
    else:
        output("Fatal error. Something bad happend. Check me.")
        print err  ### FIXME
        exit(1)

### File is empty?
if len(error_file_list) == 0:
    exit(0)

### File has non wrapped lines? Print them and exit.
if search_not_wrapped(error_file_list):
    exit(2)

### Creating list of uniq daemons
uniq_daemons = get_uniq_daemons(error_file_list)

### Creating three lists for each type of alert and fill it with alert strings
result_critical = []
result_warning = []
result_info = []
daemons_dict = {}

for daemon in uniq_daemons:

    if options.use_dict:
        limits = set_limits(dict_file_list, daemon, options.critical, options.warning, options.delta)
    else:
        limits = {'crit': options.critical, 'warn': options.warning, 'delta': options.delta}

    ### If there is nothing inside limits, we can't go on.
    if limits is None or len(limits) == 0:
        output("Logic error. Inside set_limits function. Last deamon was: %s" % daemon)
        exit(1)

    critical = int(limits['crit'])
    warning = int(limits['warn'])
    delta = int(limits['delta'])

    if warning > critical:
        output("Configuration error. Warning limit is more that Critical limit.\nYour input: %s > %s for %s" % (warning, critical, daemon))
        exit(2)

    ### Filling daemons_dict with "daemon_name : restart_count" pairs
    daemons_dict = check_delta(daemon, error_file_list, delta)

    ### If sum of all values == 0, it means what all restarts are not in our delta
    if sum(daemons_dict.values()) == 0:
        exit(0)

    ### Restart count checked against limits.
    if daemons_dict[daemon] >= critical:
        result_critical.append(daemon + " restarted " + str(daemons_dict[daemon]) + " times in " + str(delta) + " minutes.")
    elif daemons_dict[daemon] >= warning and daemons_dict[daemon] < critical:
        result_warning.append(daemon + " restarted " + str(daemons_dict[daemon]) + " times in " + str(delta) + " minutes.")
    elif daemons_dict[daemon] < warning and daemons_dict[daemon] > 0:
        result_info.append(daemon + " restarted " + str(daemons_dict[daemon]) + " times in " + str(delta) + " minutes.")

### Depending on situation it prints revelant list filled with alert strings
if len(result_critical) != 0 and len(result_warning) != 0:
    print_list(result_critical)
    print_list(result_warning)
    exit(1)
elif len(result_critical) != 0 and len(result_warning) == 0:
    print_list(result_critical)
    exit(1)
elif len(result_warning) != 0:
    print_list(result_warning)
    exit(2)
elif len(result_info) != 0:
    print_list(result_info)
    exit(3)
