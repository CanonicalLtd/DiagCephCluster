import optparse
import os
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
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    init_script = CURRENT_DIR + '/scripts/check_init.sh'
    init_type = ''
    arch_script = CURRENT_DIR + '/scripts/find_processor_architecture.sh'
    arch_type = ''

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

        cls.arch_type = self._get_arch_type(cls.connection).strip()

    def _get_init_type(self, connection):
        cmd = open(self.init_script, 'r').read()
        out, err = self._execute_command(connection, cmd)
        return out.read()

    def _get_arch_type(self, connection):
        cmd = open(self.arch_script, 'r').read()
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
        parser.add_option('-t', '--timeout', dest='timeout', default=30)
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
            cls._get_eof(output, command)
            cluster_status = output.read().split(' ')[0].strip()
        except TimeoutError as err:
            # ceph cli is not working i.e. quorum is not being established
            # hence we need to use ceph admin sockets
            return None

        return cluster_status

    @classmethod
    @timeout(10)
    def check_ceph_cli_health(cls, connection, command='sudo ceph health'):
        (output, err) = cls._execute_command(connection, 'sudo ceph health')
        status = output.read().split(' ')[0].strip()

        if status == 'HEALTH_OK':
            print 'Ceph cluster working again'
            exit()
        else:
            print "Didn't work, trying deeper probe"

    @classmethod
    @timeout(10)
    def _get_eof(cls, stream, command):
        while not stream.channel.eof_received:
            pass
        return stream.channel.eof_received

    @classmethod
    def poll_ceph_status(cls, connection, command='sudo ceph health'):
        tries = cls.options.timeout / 10
        status = None
        for i in range(tries):
            (out, err) = cls._execute_command(connection, command)
            try:
                cls._get_eof(out, command)
            except TimeoutError:
                print 'retrying status'
            else:
                status = out.read().split(' ')[0].strip()
                if status == 'HEALTH_OK':
                    return status
        return status
