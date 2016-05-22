from ConfigParser import ConfigParser
import optparse
import paramiko
import re

from helpers.exceptions import (SSHCredsNotFoundError, ConnectionFailedError,
                                TimeoutError, InitSystemNotSupportedError)
from helpers.decorators import timeout


class MonObject(object):
    def __init__(self, host, connection=None, admin_socket=None):
        self.host = host
        self.admin_socket = admin_socket
        self.connection = connection


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

        if not (cls.options.host and cls.options.user):
            msg = 'Hostname and  Username are compulsary for ssh provider,\
                   see help'
            raise SSHCredsNotFoundError(msg)

        try:
            cls.connection = cls._get_connection(cls.options.host,
                                                 cls.options.user,
                                                 cls.options.password)
        except Exception:
            raise ConnectionFailedError('Couldnot connect to host')

        cls.init_type = self._get_init_type(cls.connection).strip()
        if cls.init_type == 'none':
            raise InitSystemNotSupportedError()

    def _get_init_type(self, connection):
        cmd = open(self.init_script, 'r').read()
        out, err = self._execute_command(connection, cmd)
        return out.read()

    def _get_opt_parser(self):
        desc = 'Command line parser for CephDiagnoseTool'

        parser = optparse.OptionParser(description=desc)
        parser.add_option('-H', '--host', dest='host', default=None)
        parser.add_option('-u', '--user', dest='user', default=None)
        parser.add_option('-p', '--pass', dest='password', default=None)
        parser.add_option('-P', '--provider', dest='provider', default='ssh',
                          choices=['ssh', 'juju'],
                          help='currently supports ssh')
        return parser

    @classmethod
    def _get_connection(cls, hostname, username, password):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=hostname, username=username,
                       password=password)
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
            print "Didn't work, trying deeper probe"

    def _troubleshoot_mon_socket(self):
        '''
            Troubleshoot mon issues using monitor sockets when ceph cli
            doesnt work i.e. quorum is not being established
        '''
        self.live_machines, self.dead_machines = self._get_machine_objects()

        if len(self.dead_machines) >= len(self.live_machines):
            # TODO Network issues
            print 'Dead machines more than Live machines'
            print 'Impossible to achieve quorum, aborting'
            exit()

    def _get_machine_objects(self):
        mon_list = self._get_mon_list()
        live_machines, dead_machines = [], []

        for i in mon_list:
            try:
                connection = self._get_connection(i, self.options.user,
                                                  self.options.password)
            except ConnectionFailedError as err:
                mon = MonObject(i)
                dead_machines.append(mon)
            else:

                out, err = self._execute_command(connection,
                                                 'ls /var/run/ceph/')
                socket = self._find_mon_socket(out.read().split('\n'))
                mon = MonObject(i, connection, socket)
                live_machines.append(mon)
        return live_machines, dead_machines

    def _find_mon_socket(self, out_list):
        for i in out_list:
            if re.search(r"^ceph-mon\..*\.asok$", i.strip()) is not None:
                return i
        return None

    def _get_mon_list(self):
        ''' Parse ceph.conf and get mon list '''
        self.connection.open_sftp().get('/etc/ceph/ceph.conf', './ceph.conf')
        Config = ConfigParser()
        Config.read('./ceph.conf')
        mon_list = Config.get('global', 'mon host').split(' ')
        mon_list = [i.split(':', 1)[0] for i in mon_list]
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

                    self._restart_ceph_mon_service(mon_addr,
                                                   self.options.user,
                                                   self.options.password)

    def _restart_ceph_mon_service(self, addr, username, password):
        connection = self._get_connection(addr, username, password)
        if self.init_type in ['upstart', 'sysv-init']:
            cmd = 'sudo start ceph-mon-all'
        else:
            cmd = 'sudo systemctl stop ceph-mon.service'
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
