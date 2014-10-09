#!/usr/bin/env python

import socket
from glob import glob
from select import select
from os.path import isfile
import diamond.collector

### Global vars
cfg_paths_list = ['/usr/local/etc/', '/etc/tarantool/']
sock_timeout = 0.1
general_dict = {'show slab': ['items_used', 'arena_used', 'waste'],
                'show info': ['recovery_lag', 'recovery_run_crc_lag'],
                'show stat': ['nonexist']}


class TTCollector(diamond.collector.Collector):
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(TTCollector, self).get_default_config()
        config.update({
            'path': 'tt'
        })

        return config

    def collect(self):
        instances = self.config.get('instances')

        metrics_dict = {}
        for inst in instances:
            inst_conf_path = self.make_paths_list(cfg_paths_list, inst)
            ints_conf_dict = self.make_cfg_dict(inst_conf_path)
            metrics_dict[inst] = self.make_proc_dict(ints_conf_dict['aport'], general_dict)
            for item in metrics_dict[inst].keys():
                if metrics_dict[inst][item] != '':
                    self.publish(inst.replace('.', '_') + '.' + item.replace('.', '_'), metrics_dict[inst][item])
                else:
                    self.log.error("Cant get metric value for %s at instance %s" % (item, inst))

    def open_file(self, filename):
        """ We try to open file and copy it into list. """

        if not isfile(filename):
            self.log.error("I/O error. There is no '%s'. Check me." % filename)
        try:
            return list(open(filename))
        except Exception, err:
            self.log.exception(err)

    def make_paths_list(self, paths, inst):
        """ Make a list with paths to files. Full path to cfg and just basename for init scripts """

        paths_list_loc = []

        for path in paths:
            if glob(path + inst + '.cfg'):
                paths_list_loc.extend(glob(path + inst + '.cfg'))

        if len(paths_list_loc) > 1:
            self.log.error("Found more then one config for instance %s: %s" % (inst, paths_list_loc))
        return paths_list_loc[0]

    def make_cfg_dict(self, config):
        """ Making dict from tt\octopus cfg's """

        cfg_dict_loc = {}
        try:
            file_list = self.open_file(config)
        except Exception, err:
            self.log.exception(err)

        cfg_dict_loc = {'primary_port': '', 'aport': '', 'config': ''}
        for string in file_list:
            if 'primary_port' in string:
                cfg_dict_loc['primary_port'] = string.split()[2]
            elif 'admin_port' in string:
                cfg_dict_loc['aport'] = string.split()[2]
            cfg_dict_loc['config'] = config

        return cfg_dict_loc

    def open_socket(self, sock, timeout, host, port):
        """ We try to open socket here and catch nasty exeptions if we can't """

        try:
            sock.settimeout(timeout)
            sock.connect((host, int(port)))
            return True
        except Exception, err:
            self.log.exception(err)

        return False

    def read_socket(self, sock, timeout=1, recv_buffer=262144):
        """ Nice way to read from socket. We use select() for timeout and recv handling """

        buffer = ''
        receiving = True
        while receiving:
                ready = select([sock], [], [], timeout)
                if ready[0]:
                        data = sock.recv(recv_buffer)
                        buffer += data

                        ### Have we reached end of data?
                        for line in buffer.splitlines():
                                if '...' in line:
                                    receiving = False
                else:
                        buffer = 'check_error: Timeout after %s second' % timeout
                        receiving = False
                        self.log.error(buffer)

        for line in buffer.splitlines():
            yield line

    def get_stats(self, sock, lookup_dict, timeout=0.1, recv_buffer=262144):
        """ Parsing internal tt\octopus info from admin port """

        args_dict = {}
        for list in lookup_dict.itervalues():
            for arg in list:
                    args_dict[arg] = ''
        args_dict['recovery_lag'] = 0
        args_dict['recovery_run_crc_lag'] = 0
        args_dict['waste'] = 0
        del args_dict['nonexist']

        for command in lookup_dict.keys():
            try:
                sock.sendall(command + '\n')
                args_set = set(lookup_dict[command])
            except socket.error, err:
                self.log.exception(err)

            if command != 'show stat':
                need = len(args_set)
                got = 0
                for line in self.read_socket(sock, timeout):
                    if got < need:
                        line = line.strip().split(':', 1)
                        if line[0] in args_set or line[0] == 'check_error':
                            args_dict[line[0]] = line[1]
                            got += 1
                    else:
                        break
            else:
                for line in self.read_socket(sock, timeout):
                    if 'rps' in line:
                        line = line.split()
                        args_dict[line[0].strip(':')] = line[3]

        sock.sendall('quit\n')
        return args_dict

    def make_proc_dict(self, aport, lookup_dict, host='localhost'):
        """ Making dict from running tt\octopus """

        adm_dict_loc = {}

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.open_socket(sock, sock_timeout, host, aport)
            args_dict = self.get_stats(sock, lookup_dict, sock_timeout)
            sock.close()

            filters = {
                'items_used': lambda x: int(str(x).rsplit('.')[0]),
                'arena_used': lambda x: int(str(x).rsplit('.')[0]),
                'waste': lambda x: int(str(x).rsplit('.')[0]),
                'recovery_lag': lambda x: int(str(x).rsplit('.')[0]),
                'recovery_run_crc_lag': lambda x: int(str(x).rsplit('.')[0]),
                'config': lambda x: x.strip(' "'),
                'primary_port': lambda x: x.strip(' "'),
            }

            for key in set(args_dict.keys()) & set(filters.keys()):
                if args_dict[key] != '' and args_dict[key] is not 0:
                    args_dict[key] = filters[key](args_dict[key])

            adm_dict_loc = args_dict
        except Exception, err:
            self.log.exception(err)

        return adm_dict_loc
