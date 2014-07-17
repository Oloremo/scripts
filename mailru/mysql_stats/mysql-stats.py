#!/usr/bin/env python

import time
import MySQLdb
import logging
import socket
from optparse import OptionParser
from glob import glob
from os import chdir
from os.path import isfile
from daemon import Daemon

### Gotta catch 'em all!
usage = "usage: %prog [--const path/to/file] [--log path/to/log] [--error_file path/to/file] [--log_level LEVEL] [-i INTERVAL] -a {start,stop,restart,status}"
parser = OptionParser(usage=usage)
parser.add_option("--const", type="str", dest="MYSQL_CONSTANTS", default="/usr/local/etc/mysql-status-constants.lst",
                  help="File with list of mysql variables to check. Default: /usr/local/etc/mysql-status-constants.lst")
parser.add_option("--log", type="str", dest="log_file", default="/var/log/mailru/mysql-status.log",
                  help="Path to log file. Default: /var/log/mailru/mysql-status.log")
parser.add_option("--error_log", type="str", dest="error_file", default="/var/tmp/mysql-status.err",
                  help="Path to error log file. Default: /var/tmp/mysql-status.err")
parser.add_option('--log_level', type='choice', action='store', dest='loglevel', default='INFO',
                  choices=['INFO', 'WARNING', 'CRITICAL', 'DEBUG'], help='Default log level. Choose from: INFO, WARNING, CRITICAL and DEBUG')
parser.add_option("-i", "--interval", type="int", dest="INTERVAL", default='300',
                  help="Check interval in seconds. Default: 300")
parser.add_option('-a', '--action', type='choice', action='store', dest='action',
                  choices=['start', 'stop', 'restart', 'status'], help='Action triger: start, stop, restart or status')

(opts, args) = parser.parse_args()

### Global vars
log_file = opts.log_file
error_file = opts.error_file
loglevel = logging.getLevelName(opts.loglevel)
MYSQL_INIT_PATH = ['mysql-*']
LOOKUP_LIST = ['datadir', 'socket']
INTERVAL = opts.INTERVAL
MYSQL_CONSTANTS = opts.MYSQL_CONSTANTS
CARBON_SERVER = '10.255.1.107'
CARBON_PORT = 2003

class MyDaemon(Daemon):

    def init(self):
        self.interval = INTERVAL
        self.stats_last = {}
        self.first_run = True
        self.init_logger()
        self.hostname = socket.gethostname().split(".", 1)[0]
        self.logger.info('Starting...')

    def init_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        mainlog = logging.FileHandler(log_file)
        error_log = logging.FileHandler(error_file, mode='w')

        mainlog.setLevel(loglevel)
        error_log.setLevel(logging.CRITICAL)

        format = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        eformat = logging.Formatter('%(asctime)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        mainlog.setFormatter(format)
        error_log.setFormatter(eformat)

        logger.addHandler(mainlog)
        logger.addHandler(error_log)

        self.logger = logger

    def open_file(self, filename):
        """ We try to open file and copy it into list. """

        if not isfile(filename):
            self.logger.critical("I/O error. There is no '%s'. Check me." % filename)
            raise Exception('NO_FILE')
        try:
            return list(open(filename))
        except IOError, err:
            self.logger.critical("I/O error. Can't open file '%s'. Check me." % filename)
            self.logger.critical("Error %s: %s" % (err.errno, err.strerror))
            raise Exception('IO_ERROR')
        except:
            raise Exception

    def get_all_mysql(self):
        """ Making list of all mysql init scripts """

        self.logger.info('Making list of all mysql init scripts...')
        inits = []
        chdir('/etc/init.d')
        for path in MYSQL_INIT_PATH:
            if glob(path):
                inits.extend(glob(path))

        self.inits = inits

    def load_mysql_constants(self, file):

        self.logger.info('Loading mysql constants from %s' % file)
        mysql_keys_dict = {}

        mysql_keys = self.open_file(file)
        for line in mysql_keys:
            line = line.split()
            mysql_keys_dict[line[0].lower()] = line[1]

        self.mysql_keys_dict = mysql_keys_dict

    def make_mysql_dict(self):
        """ Make dict of all mysql with datadir and socket args """

        self.logger.info('Making dict of all mysql instances...')
        chdir('/etc/init.d')
        mysql_dict = {}

        for init in self.inits:
            mysql_args_dict = {}
            file = self.open_file(init)
            for line in file:
                line = line.split('=')
                if line[0] in LOOKUP_LIST:
                    mysql_args_dict[line[0]] = line[1].strip()
            mysql_dict[init] = mysql_args_dict

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug('Mysql dict:')
            for key, value in mysql_dict.items():
                self.logger.debug('%s = %s' % (key, value))
        self.mysql_dict = mysql_dict

    def connect(self, mysql_dict):
        ### Add try... exept
        self.logger.info('Connecting to socket %s' % mysql_dict['socket'])
        return MySQLdb.connect(unix_socket=mysql_dict['socket'], db='information_schema')

    def get_stats(self, db):

        self.logger.info('Generating stats dict for %s' % db)
        stats = {}
        time_now = int(time.time())

        dcur = db.cursor(MySQLdb.cursors.DictCursor)
        dcur.execute('select * from GLOBAL_STATUS')
        rows = dcur.fetchall()

        for line in rows:
            key = line['VARIABLE_NAME'].lower()
            if key in self.mysql_keys_dict.keys():
                if (self.mysql_keys_dict[key] == 'number' or 'counter'):
                    value = float(line['VARIABLE_VALUE'])
                else:
                    value = line['VARIABLE_VALUE']

                if ((self.mysql_keys_dict[key] == 'counter') and
                   (self.stats_last) and
                   (time_now - self.stats_last[key]['time'] > 5) and
                   (value >= self.stats_last[key]['value'])):

                    per_min_value = self.interval * (value - self.stats_last[key]['value']) / (time_now - self.stats_last[key]['time'])
                    stats[key] = {'key': key, 'value': per_min_value, 'time': time_now}
                    self.first_run = False
                elif self.mysql_keys_dict[key] == 'number':
                    stats[key] = {'key': key, 'value': value, 'time': time_now}
                else:
                    stats[key] = {'key': key, 'value': value, 'time': time_now}
                    if not self.first_run:
                        self.logger.warning('Strange keys: %s = %s' % (key, value))

        self.stats = stats
        if not self.stats_last:
            self.stats_last = dict(self.stats)

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug('Stats dict:')
            for key, value in stats.items():
                self.logger.debug('%s = %s' % (key, value))

    def send_data(self):

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.logger.info('Sending data to graphite')
        for key in self.stats.keys():
            self.logger.info('my.mysql.%s.%s %s %s' % (self.hostname, self.stats[key]['key'], self.stats[key]['value'], self.stats[key]['time']))
            sock.sendto('my.mysql.%s.%s %s %s' % (self.hostname, self.stats[key]['key'], self.stats[key]['value'], self.stats[key]['time']), (CARBON_SERVER, CARBON_PORT))

    def run(self):
        self.init()
        self.get_all_mysql()
        self.make_mysql_dict()
        self.load_mysql_constants(MYSQL_CONSTANTS)

        while True:
            for instance in self.mysql_dict.keys():
                db = self.connect(self.mysql_dict[instance])
                self.get_stats(db)
                if not self.first_run:
                    self.send_data()
            self.logger.info('Sleeping for %s' % self.interval)
            time.sleep(self.interval)

if __name__ == "__main__":
        daemon = MyDaemon('/var/run/mysql-stats.pid', stdout=log_file, stderr=log_file)
        if opts.action == 'start':
            daemon.start()
            daemon.run()
        elif opts.action == 'stop':
            daemon.stop()
        elif opts.action == 'restart':
            daemon.restart()
