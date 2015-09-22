#!/usr/bin/env python

import time
import requests
import logging
import os
from datetime import datetime
from optparse import OptionParser
from sys import exit

### Gotta catch 'em all!
usage = "usage: %prog "

parser = OptionParser(usage=usage)
parser.add_option("--days", dest="retention_days", type="int", default=7,
                  help="All snapshots which are more then this daysago will be  deleted. Default: 7")
parser.add_option('--log_level', type='choice', action='store', dest='loglevel', default='INFO',
                  choices=['INFO', 'WARNING', 'CRITICAL', 'DEBUG'], help='Log level. Choose from: INFO, WARNING, CRITICAL and DEBUG. Default is INFO')

(opts, args) = parser.parse_args()

### Global
hr_date_now = time.strftime("%Y.%m.%d")
ut_date_now = time.mktime(datetime.strptime(hr_date_now, "%Y.%m.%d").timetuple())
base_url = "http://myes1.i:9200"

def get_snap_status():

    url = "%s/_snapshot/my_backup/_all" % base_url
    r = requests.get(url)
    if r.status_code != 200:
        logger.critical('Cant get snapshot info from ES. Return code is: %s ' % r.status_code)
        logger.critical('Error was: %s' % r.text)
        exit(1)
    else:
        return r.json()['snapshots']

def del_old_snaps(retention_days, snaps_info):

    retention_ut = retention_days * 86400
    for snap in snaps_info:
        url = "%s/_snapshot/my_backup/%s?wait_for_completion=true" % (base_url, snap['snapshot'])
        name, bk_date = snap['snapshot'].split('-')
        ut_date_of_bk = time.mktime(datetime.strptime(bk_date, "%Y%m%d").timetuple())
        logger.debug('Snapshot: "%s", retention_days: "%s" -- %s < %s, result: %s' %
                     (snap['snapshot'], retention_days, ut_date_of_bk, ut_date_now - retention_ut, ut_date_of_bk < ut_date_now - retention_ut))
        if ut_date_of_bk < ut_date_now - retention_ut:
            logger.info('Deleting snapshot "%s", older than %s days' % (snap['snapshot'], retention_days))
            r = requests.delete(url)
            if r.status_code != 200:
                logger.critical('Deleting snapshot %s is failed. Return code is: %s ' % (snap['snapshot'], r.status_code))
                logger.critical('Error was: %s' % r.text)

def get_all_indexes():

    url = "%s/_cat/indices" % base_url
    r = requests.get(url)
    indexes = []

    if r.status_code != 200:
        logger.critical('Cant get indexes info from ES. Return code is: %s ' % r.status_code)
        logger.critical('Error was: %s' % r.text)
        exit(1)
    else:
        for index in r.text.splitlines():
            indexes.append(index.split()[2])
        return indexes

def create_snap(indexes):

    for index in indexes:
        url = "%s/_snapshot/my_backup/%s-%s?wait_for_completion=true" % (base_url, index, hr_date_now)
        data = '{"indices": "%s" }' % index
        logger.info('Starting creation of snapshot for index "%s".' % index)

        r = requests.put(url, data=data)
        if r.status_code != 200:
            logger.critical('Creation of snapshot for index "%s" is failed. Return code is: %s ' % (index, r.status_code))
            logger.critical('Error was: %s' % r.text)
        else:
            logger.info('Creation of snapshot for index "%s" is completed.' % index)

###Logger init
loglevel = logging.getLevelName(opts.loglevel)
logger = logging.getLogger('DELME')
logger.setLevel(logging.DEBUG)

error_log = logging.FileHandler('/var/tmp/es-backup.txt', mode='a')
error_log.setLevel(logging.CRITICAL)
format = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
eformat = logging.Formatter('%(asctime)s  %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
error_log.setFormatter(eformat)
logger.addHandler(error_log)

if not os.path.exists("/var/log/mailru"):
    logger.critical('Path "/var/log/mailru" is not exists - unable to start.')
    exit(1)
else:
    mainlog = logging.FileHandler('/var/log/mailru/es-backup.log')
    mainlog.setLevel(loglevel)
    mainlog.setFormatter(format)
    logger.addHandler(mainlog)

logger.info('=====================================================================================================')
logger.info('Started')

snaps_info = get_snap_status()
indexes = get_all_indexes()
create_snap(indexes)
del_old_snaps(opts.retention_days, snaps_info)

logger.info("Finished")
