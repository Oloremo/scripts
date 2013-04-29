from sys import exit
from datetime import datetime, timedelta
from time import strptime

filename = 'ratelimit_error.txt'
regexp = "restarted with exit code"
delta = 160
critical = 5
warning = 3

def search_not_wrapped(list):
    """ Looking through list and look for non wrapped lines """
    match = 0
    wrong_lines = []
    for string in list:
        if not regexp in string:
            match += 1
            wrong_lines.append(string + "<br>")
    if match != 0:
        print "There is non wrapped lines inside " + filename
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
    timestamp = datetime.strptime(timestamp,"%Y.%m.%d-%H.%M")
    if date_now - timestamp <= timedelta (minutes = delta):
        return True
    else:
        return False

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """
    for string in list:
        print string

### Copy file into list. 
file = list(open(filename))

### File is empty?
if len(file) == 0:
    exit(0)

### File has non wrapped lines? Print them and exit.
if search_not_wrapped(file):
    exit(1)

### Creating list of uniq daemons
uniq_daemons = get_uniq_daemons(file)

### Filling daemons_dict with "daemon_name : restart_count" pairs
daemons_dict = {}
for daemon in uniq_daemons:
    daemons_dict[daemon] = 0
    for string in file:
         if daemon in string:
            string = string.split()
            timestamp = string[0]
            if compare_timestamp(timestamp, delta):
                daemons_dict[daemon] += 1

### Creating three lists for each type of alert and fill it with alert strings 
result_critical = []
result_warning  = []
result_info     = []
for daemon in daemons_dict.iterkeys():
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