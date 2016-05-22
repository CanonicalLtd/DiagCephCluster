from ConfigParser import ConfigParser
import optparse
import os
import paramiko
import re

from helpers.exceptions import (SSHCredsNotFoundError, ConnectionFailedError,
                                TimeoutError, InitSystemNotSupportedError)
from helpers.decorators import timeout


class MonObject(object):
    def __init__(self, host, ssh_status, connection=None, admin_socket=None):
        self.host = host
        self.admin_socket = admin_socket
        self.connection = connection
        self.ssh_status = ssh_status
        self.mon_id = self.get_id(admin_socket)
        self.is_monmap_correct = False

    def get_id(self, admin_socket):
        if admin_socket is None:
            return None

        mon_id = admin_socket[9:]
        mon_id = mon_id[:-5]
        return mon_id


class TroubleshootCeph(object):
    '''
        TroubleshootCeph Class to be called to diagnose a ceph cluster
    '''
    GOOD_HEALTH = ['HEALTH_OK']
    BAD_HEALTH = ['HEALTH_WARN']
    init_script = './scripts/check_init.sh'
    init_type = ''

    def __init__(self):
        self.parser = self._get_opt_parser()
        cls = TroubleshootCeph
        cls.options, cls.arguments = self.parser.parse_args()

        if cls.options.provider == 'juju':
            raise NotImplementedError('#TODO Feature')

        if (not (cls.options.host and cls.options.user) and
            not (cls.options.host and cls.options.ssh_key and
                 cls.options.user)):
            msg = 'Credentials insufficient, see help'
            raise SSHCredsNotFoundError(msg)

        try:
            cls.connection = cls._get_connection(cls.options.host)
        except Exception as err:
            print err
            raise ConnectionFailedError('Couldnot connect to host')

        cls.init_type = self._get_init_type(cls.connection).strip()
        if cls.init_type == 'none':
            raise InitSystemNotSupportedError()

    def _get_init_type(self, connection):
        cmd = open(self.init_script, 'r').read()
        out, err = self._execute_command(connection, cmd)
        return out.read()

    def _get_opt_parser(self):
        desc = ('Command line parser for CephDiagnoseTool \n'
                'Login method supported are: \n'
                'username + password + hostname, '
                'username + hostname + ssh_key_location, '
                'juju #TODO')

        parser = optparse.OptionParser(description=desc)
        parser.add_option('-H', '--host', dest='host', default=None)
        parser.add_option('-u', '--user', dest='user', default=None)
        parser.add_option('-p', '--pass', dest='password', default=None)
        parser.add_option('-P', '--provider', dest='provider', default='ssh',
                          choices=['ssh', 'juju'],
                          help='currently supports ssh')
        parser.add_option('-k', '--ssh_key', dest='ssh_key', default=None)
        return parser

    @classmethod
    def _get_connection(cls, hostname):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if cls.options.ssh_key is None:
            client.connect(hostname=hostname, username=cls.options.user,
                           password=cls.options.password)
        else:
            k = paramiko.RSAKey.from_private_key_file(cls.options.ssh_key)
            client.connect(hostname=hostname, username=cls.options.user,
                           pkey=k)
        return client

    @classmethod
    def _execute_command(cls, connection, command):
        (stdin, stdout, stderr) = connection.exec_command(command)
        return (stdout, stderr)

    def start_troubleshoot(self):
        cls = TroubleshootCeph
        command = 'sudo ceph health'
        (output, err) = cls._execute_command(cls.connection, command)
        cluster_status = None

        try:
            self._get_eof(output, command)
            cluster_status = output.read().split(' ')[0].strip()
        except TimeoutError as err:
            # ceph cli is not working i.e. quorum is not being established
            # hence we need to use ceph admin sockets
            return None

        return cluster_status

    @classmethod
    @timeout(10)
    def check_ceph_cli_health(cls, connection, command='ceph health'):
        (output, err) = cls._execute_command(connection, 'sudo ceph health')
        status = output.read().split(' ')[0].strip()

        if status == 'HEALTH_OK':
            print 'Ceph cluster working again'
            exit()
        elif status == 'HEALTH_WARN':
            print "Didn't work, trying deeper probe"

    @timeout(10)
    def _get_eof(self, stream, command):
        while not stream.channel.eof_received:
            pass
        return stream.channel.eof_received


class TroubleshootCephMon(TroubleshootCeph):
    def __init__(self, is_ceph_cli):
        self.is_ceph_cli = is_ceph_cli

    def troubleshoot_mon(self):
        '''
            Based on ceph mon quorum status  troubleshoot using either
            ceph cli or ceph mon admin sockets
        '''

        if self.is_ceph_cli:
            self._troubleshoot_mon_cli()
        else:
            self._troubleshoot_mon_socket()

    def _troubleshoot_mon_cli(self):
        '''
            Troubleshoot mon issues using ceph cli i.e. the quorum has been
            established
        '''

        print 'MON Status : ', cluster_status
        print '\nProbable cause Ceph mon service not running in some machines'
        print 'Try & start service in the machines not in quorum',
        print '(yes/no) (default no)?',
        response = raw_input()

        if response != 'yes':
            print 'not proceeding with updating machines, aborting'
            return

        # Start ceph cli check_list
        self._restart_dead_mon_daemons()

        try:
            self.check_ceph_cli_health(self.connection)
        except TimeoutError:
            print "Restarting servers not in quorum didn't work,"
            print 'trying deeper probe'

        # TODO Clock Skew

        self.machines = self._get_machine_objects()

        print 'Inject correct monmap to machines with incorrect monmap?'
        print '(yes/no) (default no)?',
        response = raw_input()

        if response != 'yes':
            print 'not proceeding with updating machines, aborting'
            return

        monmap_loc = self._find_correct_monmap(self.machines)
        if monmap_loc is None:
            print 'No Mon has correct monmap, recovery impossible, aborting'
            return

        self._inject_mon_map(monmap_loc, self.machines)

        try:
            self.check_ceph_cli_health(self.connection)
        except TimeoutError:
            print "Injecting Monmap didn't work, probably network issue"

    def _inject_mon_map(self, monmap_loc, machine_list):
        for machine in machine_list:
            if (machine.ssh_status == 'LIVE' and
               machine.is_monmap_correct is False):
                print 'Injecting monmap to: ' + machine.host

                self._restart_ceph_mon_service('stop', machine.host)

                machine.connection.open_sftp().put(monmap_loc,
                                                   '/tmp/monmap')
                cmd = 'sudo ceph-mon -i ' + machine.mon_id +\
                    ' --inject-monmap /tmp/monmap'

                self._execute_command(machine.connection, cmd)

                self._restart_ceph_mon_service('start', machine.host)

    def _find_correct_monmap(self, machine_list):
        mon_host = []
        for machine in machine_list:
            mon_host.append(machine.host)
        mon_host.sort()
        correct_mon_host = None

        for machine in machine_list:
            if (machine.ssh_status == 'LIVE' and
               machine.admin_socket is not None):

                    cmd = 'sudo ceph --admin-daemon /var/run/ceph/' +\
                        machine.admin_socket + ' mon_status'
                    out, err = self._execute_command(machine.connection, cmd)
                    monmap = eval(out.read())['monmap']['mons']
                    monmap_host = sorted([i['addr'].split(':')[0]
                                         for i in monmap])
                    if monmap_host == mon_host:
                        correct_mon_host = machine if correct_mon_host\
                            is None else correct_mon_host
                        machine.is_monmap_correct = True

        if correct_mon_host is None:
            return None

        loc = '/tmp/monmap'
        self._save_monmap(correct_mon_host, loc)
        return loc

    def _save_monmap(self, mon_host, loc):

        self._restart_ceph_mon_service('stop', mon_host.host)

        cmd = 'sudo ceph mon getmap -o /tmp/monmap'
        out, err = self._execute_command(mon_host.connection, cmd)
        mon_host.connection.open_sftp().get('/tmp/monmap', loc)
        self._restart_ceph_mon_service('start', mon_host.host)

    def _troubleshoot_mon_socket(self):
        '''
            Troubleshoot mon issues using monitor sockets when ceph cli
            doesnt work i.e. quorum is not being established
        '''
        # TODO
        pass

    def _get_machine_objects(self):
        mon_list = self._get_mon_list()
        machines = []

        for i in mon_list:
            try:
                connection = self._get_connection(i)
            except ConnectionFailedError as err:
                dead_mon = MonObject(i, 'DEAD')
                machines.append(dead_mon)
            else:

                out, err = self._execute_command(connection,
                                                 'ls /var/run/ceph/')
                socket = self._find_mon_socket(out.read().split('\n'))
                live_mon = MonObject(i, 'LIVE', connection, socket)
                machines.append(live_mon)
        return machines

    def _find_mon_socket(self, out_list):
        for i in out_list:
            if re.search(r"^ceph-mon\..*\.asok$", i.strip()) is not None:
                return i
        return None

    def _get_mon_list(self):
        ''' Parse ceph.conf and get mon list '''
        self.connection.open_sftp().get('/etc/ceph/ceph.conf',
                                        '/tmp/ceph.conf')
        Config = ConfigParser()
        Config.read('/tmp/ceph.conf')
        mon_list = Config.get('global', 'mon host').split(' ')
        mon_list = [i.split(':', 1)[0] for i in mon_list]
        os.system('rm /tmp/ceph.conf')
        return mon_list

    def _restart_dead_mon_daemons(self):
        (output, err) = self._execute_command(self.connection,
                                              'sudo ceph mon_status')

        mon_status = eval(output.read())
        quorum_list = mon_status['quorum']
        mon_list = mon_status['monmap']['mons']

        if len(quorum_list) != len(mon_list):
            for mon in mon_list:
                if mon['rank'] not in quorum_list:
                    print '\n' + mon['name'] + ' not in quorum list,',
                    print 'restarting ceph services'
                    mon_addr = mon['addr'].split(':')[0]

                    self._restart_ceph_mon_service('start', mon_addr)

    def _restart_ceph_mon_service(self, cmd, addr):
        connection = self._get_connection(addr)
        if self.init_type in ['upstart', 'sysv-init']:
            cmd = 'sudo ' + cmd + ' ceph-mon-all'
        else:
            cmd = 'sudo systemctl ' + cmd + ' ceph-mon.service'
        self._execute_command(connection, cmd)
        print addr + ' Restart successful'


if __name__ == "__main__":
    TroubleshootCeph = TroubleshootCeph()
    cluster_status = TroubleshootCeph.start_troubleshoot()
    if cluster_status == 'HEALTH_OK':
        print 'All good up here :-)'
        exit()
    elif cluster_status is None:
        TroubleshootCephMon = TroubleshootCephMon(is_ceph_cli=False)
    else:
        TroubleshootCephMon = TroubleshootCephMon(is_ceph_cli=True)

    TroubleshootCephMon.troubleshoot_mon()
