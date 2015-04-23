#!/usr/bin/env python

import subprocess
import re
import MySQLdb
import simplejson as json
from sys import exit, stdout
from os.path import isfile
from netifaces import interfaces, ifaddresses
from optparse import OptionParser

### Gotta catch 'em all!
usage = "usage: %prog "

parser = OptionParser(usage=usage)
parser.add_option("--conf", dest="config", type="str", default="/etc/memc.conf",
                  help="Config file. Default: /etc/memc.conf")
parser.add_option("--json", action="store_true", dest="json_output_enabled",
                  help="Enable json output for some checks")

(opts, args) = parser.parse_args()

port_pattern = '-p ([^ ]\d+)'
name_pattern = '\-([^-.]+)\.pid'

def output(line):
    stdout.write(str(line) + "<br>")
    stdout.flush()

def print_list(list):
    """ Eh... well... it's printing the list... string by string... """

    for string in list:
        output(string)

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

def make_memc_dict(port_pattern, name_pattern):
    """ Making list of a running memcached process to parse after """

    ps = subprocess.Popen(['ps', '-U', 'memcached', '-o', 'args'], stdout=subprocess.PIPE).communicate()[0]
    memc_dict = {}
    pport = re.compile(port_pattern)
    pname = re.compile(name_pattern)
    for line in ps.splitlines():
        if 'memcached' in line:
            memc_dict[pname.findall(line)[0]] = pport.findall(line)[0]
    return memc_dict

def getip():
    """ Returns list of ips of this server """

    ip_list = []
    for interface in interfaces():
        if 2 in ifaddresses(interface):
            if ifaddresses(interface)[2][0]['addr'].startswith('10.') and not ifaddresses(interface)[2][0]['addr'].startswith('10.34'):
                ip_list.append(ifaddresses(interface)[2][0]['addr'])

    if not ip_list:
        output("Can't get server ip list. Check me.")
        exit(1)
    else:
        return ip_list

def check_pinger(memc_dict, config_file):
    """ Check if octopus\tt on this host is in pinger database """

    pinger_list = []
    to_json = {}
    ip_list = getip()

    config = load_config(config_file, 'pinger')

    ### Connect to db and check remote_stor_ping table for ip:port on this host
    try:
        db = MySQLdb.connect(host=config['host'], user=config['user'], passwd=config['pass'], db=config['db'])
        cur = db.cursor()
        for ip in ip_list:
            for name, port in memc_dict.items():
                cur.execute("SELECT * FROM remote_stor_ping WHERE connect_str='%s:%s' and typ='%s';" % (ip, port, 'memcached'))
                if int(cur.rowcount) is 0:
                    pinger_list.append('Memcached "%s" with ip:port %s:%s not found in pinger database!' % (name, ip, port))
                    to_json[name] = {'title': name, 'ip': ip, 'port': port, 'proto': 'memcached'}
    except Exception, err:
            output('MySQL error. Check me.')
            print err
            ### We cant print exeption error here 'cos it can contain auth data
            exit(1)

    if opts.json_output_enabled:
        print json.dumps(to_json)
    elif pinger_list:
        print_list(pinger_list)
        exit(2)

memc_dict = make_memc_dict(port_pattern, name_pattern)
check_pinger(memc_dict, opts.config)
