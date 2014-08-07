#!/usr/bin/env python

from os.path import isfile
import socket
from sys import exit

logfile = '/var/log/nginx/searchdump.video_access.log'
nginx_ip_file = '/etc/nginx/searchdump_yaip.list'

def open_file(filename):
    """ We try to open file and copy it into list. """

    if not isfile(filename):
        print "I/O error. There is no '%s'. Check me." % filename
        raise Exception('NO_FILE')
    try:
        return list(open(filename))
    except IOError, err:
        print "I/O error. Can't open file '%s'. Check me." % filename
        print "Error %s: %s" % (err.errno, err.strerror)
        raise Exception('IO_ERROR')
    except:
        raise Exception

def make_ipset(logfile):

    yalines = []
    try:
        file = open_file(logfile)
    except Exception, err:
        if 'NO_FILE' in err:
            exit(2)
        elif 'IO_ERROR' in err:
            exit(1)
        else:
            print "Unhandled exeption. Check me."
            exit(1)

    for line in file:
        if 'GET /yandex/' in line:
            yalines.append(line)

    ipset = set([line.split()[0] for line in yalines])
    return ipset

def make_host_ip_dict(ipset):

    host_ip_dict = {}
    for ip in ipset:
        host_ip_dict[ip] = socket.gethostbyaddr(ip)[0]

    return host_ip_dict

def check_ip(host_ip_dict):

    goodip = []
    for ip in host_ip_dict.keys():
        try:
            if ip == socket.gethostbyname(host_ip_dict[ip]):
                goodip.append(ip)
        except socket.gaierror, err:
            continue
        except Exception, err:
            print "Some Network error happend. Check me."
            print err
            exit(1)

    return goodip

def make_conf(good_ip, nginx_ip_file):

    try:
        file = open(nginx_ip_file, 'w')
    except Exception, err:
        print "Some I\O error happend. Check me."
        print err
        exit(1)
    for ip in good_ip:
        try:
            file.write('allow   %s;\n' % ip)
        except Exception, err:
            print "Some I\O error happend. Check me."
            print err
            exit(1)

ipset = make_ipset(logfile)
if not ipset:
    exit(0)
host_ip_dict = make_host_ip_dict(ipset)
good_ip = check_ip(host_ip_dict)
make_conf(good_ip, nginx_ip_file)
