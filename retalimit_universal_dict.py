from sys import exit                      # for exit codes
from datetime import datetime, timedelta  # for getting a date
from time import strptime                 # for time convertation
from optparse import OptionParser         # for usage
from os.path import isfile                # for OS file check

### TODO
# print format
# open file
# usage 


### Gotta catch 'em all!
usage = "usage: %prog [-f /path/to/file] [-l /path/to/limits] [-c critical_limit] [-w warning_limit] [-d delta_in_minutes]"
parser = OptionParser(usage=usage)
parser.add_option("-f", "--file", dest="error_file", default="/var/tmp/error.txt", help="Path to file to check. Default: /var/tmp/error.txt")
parser.add_option("-l", "--limits", dest="dict_file", default="/etc/snmp/bin/ratelimit_dict.txt", help="Path to file to file with custom limits. Default: /etc/snmp/bin/ratelimit_dict.txt")
parser.add_option("-c", "--crit", type="int", dest="critical", default=5, help="Critical limit. Default: 5")
parser.add_option("-w", "--warn", type="int", dest="warning", default=3, help="Warning limit. Default: 3")
parser.add_option("-d", "--delta", type="int", dest="delta", default=60, help="Delta in minutes. Default: 60")
parser.add_option("-s", "--string", dest="regexp", default="restarted with exit code", help="May be we'll need to change it someday...")

(options, args) = parser.parse_args()
if options.warning >= options.critical:
        parser.error("Warning limit can't be more that Critical limit")

### Assign variables
error_file = options.error_file
dict_file  = options.dict_file
error_file = "ratelimit_error.txt" # delme
dict_file  = "daemon_dict.txt" # delme
regexp     = options.regexp

### Because of python 2.4 didn't have datetime.strptime we use this shit
if hasattr(datetime, 'strptime'):
    ### Python 2.6+
    strptime_loc = datetime.strptime
else:
    ### Python 2.4 equivalent
    strptime_loc = lambda date_string, format: datetime(*(strptime(date_string, format)[0:6]))

### Stop! Function time!
def search_not_wrapped(list):
    """ Looking through list and look for non wrapped lines """
    match = 0
    wrong_lines = []
    for string in list:
        if not regexp in string:
            match += 1
            wrong_lines.append(string + "<br>")
    if match != 0:
        print "There is non wrapped lines inside " + error_file
        for string in wrong_lines:
            print string
        return True
    else:
        return False
        
def get_uniq_daemons(list):
    """ Sorting all demons and return list of uniq daemons """
    uniq_daemons = []
    for string in list:
        words = string.split()
        uniq_daemons.append(words[2])
    return sorted(set(uniq_daemons))

def compare_timestamp(timestamp, delta):
    """ Subtract daemon crush timestamp from current time and compare result with delta """
    date_now = datetime.now()
    timestamp = strptime_loc(timestamp,"%Y.%m.%d-%H.%M")
    if date_now - timestamp <= timedelta (minutes = delta):
        return True
    else:
        return False

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """
    for string in list:
        print string

def set_limits(list, daemon, def_critical, def_warning, def_delta):
    daemon =  daemon.rstrip('0123456789. ')
    for line in list:
        if not line.lstrip().startswith('#'):
            if daemon in line:
                line = line.split()
                limits = {'crit' : line[1], 'warn' : line[2], 'delta' : line[3]}
                for key, value in limits.iteritems():
                    if value == "0":
                        if key == 'crit':
                            limits[key] = def_critical
                        elif key == 'warn':
                             limits[key] = def_warning
                        elif key == 'delta':
                             limits[key] = def_delta
                return limits
            #break
    limits = {'crit' : def_critical, 'warn' : def_warning, 'delta' : def_delta}
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
if isfile(error_file): 
    try: error_file_list = list(open(error_file))
    except IOError as error:
        print "Can't open file: " + error_file + ". Check me."
        print error
        exit(1)
else:
    print "There is no " + error_file + ". Check me."
    exit(3)

### Check existence of a file, then copy file into list. 
if isfile(dict_file): 
    try: dict_file_list = list(open(dict_file))
    except IOError as error:
        print "Can't open file: " + dict_file + ". Check me."
        print error
        exit(1)
else:
    print "There is no " + dict_file + ". Check me."
    exit(3)

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
result_warning  = []
result_info     = []
daemons_dict    = {}
for daemon in uniq_daemons:

    new_limits = set_limits(dict_file_list, daemon, options.critical, options.warning, options.delta)

    if new_limits == None:
        print "Script logic error in set_limits. Last deamon was:", daemon
        exit(1)

    print daemon, new_limits
    critical   = int(new_limits['crit'])
    warning    = int(new_limits['warn'])
    delta      = int(new_limits['delta'])

    ### Filling daemons_dict with "daemon_name : restart_count" pairs
    daemons_dict = check_delta(daemon, error_file_list, delta)

    ### If sum of all values == 0, it means what all restarts are not in our delta
    if sum(daemons_dict.values()) == 0:
        exit(0)

    ### Restart count checked against limits.
    if daemons_dict[daemon]   >= critical:
        result_critical.append(daemon + " restarted " + str(daemons_dict[daemon]) + " times in " + str(delta) + " minutes.") 
    elif daemons_dict[daemon] >= warning and daemons_dict[daemon] < critical:
        result_warning.append( daemon + " restarted " + str(daemons_dict[daemon]) + " times in " + str(delta) + " minutes.") 
    elif daemons_dict[daemon]  < warning and daemons_dict[daemon] > 0:
        result_info.append(    daemon + " restarted " + str(daemons_dict[daemon]) + " times in " + str(delta) + " minutes.") 

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