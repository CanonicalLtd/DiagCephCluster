import optparse
import paramiko

from helpers.exceptions import (SSHCredsNotFoundError, ConnectionFailedError,
                                TimeoutError, InitSystemNotSupportedError)
from helpers.decorators import timeout


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
            # ceph cli is not working we need to use ceph admin sockets
            return None

        return cluster_status

    @classmethod
    def check_ceph_cli_health(cls, connection):
        (output, err) = cls._execute_command(connection, 'sudo ceph health')
        return output.read().split(' ')[0].strip()

    @timeout(10)
    def _get_eof(self, stream, command):
        while not stream.channel.eof_received:
            pass
        return stream.channel.eof_received


class TroubleshootCephMon(TroubleshootCeph):
    def __init__(self, is_ceph_cli):
        self.is_ceph_cli = is_ceph_cli

    def troubleshoot_mon(self):
        if self.is_ceph_cli:
            self.troubleshoot_mon_cli()
        else:
            self.troubleshoot_mon_socket()

    def troubleshoot_mon_cli(self):
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

        if self.check_ceph_cli_health(self.connection) == 'HEALTH_OK':
            print 'Ceph cluster working again'
            return

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

    def troubleshoot_mon_socket(self):
        print '# TODO'

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
