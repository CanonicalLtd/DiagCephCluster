import json
import optparse
import os
import paramiko
import re
import subprocess
import sys

from helpers.exceptions import (SSHCredsNotFoundError, ConnectionFailedError,
                                TimeoutError, InitSystemNotSupportedError,
                                JujuInstallationNotFoundError)
from helpers.decorators import timeout
from helpers.helpers import MyStr


class JujuCephMachine(object):
    def __init__(self, name, id, public_addr, hostname, has_osd=False,
                 has_mon=False, internal_ip=None):
        self.name = name
        self.id = id
        self.public_addr = public_addr
        self.hostname = hostname
        self.has_osd = has_osd
        self.has_mon = has_mon
        self.internal_ip = internal_ip


class TroubleshootCeph(object):
    '''
        TroubleshootCeph Class to be called to diagnose a ceph cluster
    '''
    GOOD_HEALTH = ['HEALTH_OK']
    BAD_HEALTH = ['HEALTH_WARN']
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    init_script = CURRENT_DIR + '/scripts/check_init.sh'
    arch_script = CURRENT_DIR + '/scripts/find_processor_architecture.sh'

    def __init__(self):
        self.parser = self._get_opt_parser()
        cls = TroubleshootCeph
        cls.options, cls.arguments = self.parser.parse_args()
        cls.juju_version = None
        cls.timeout = cls.options.timeout
        if not hasattr(cls, 'cli_down'):
            cls.cli_down = False

        if cls.options.provider == 'juju':
            cls.juju_version = self._find_juju_version()
        elif (not (cls.options.host and cls.options.user) and
              not (cls.options.host and cls.options.ssh_key and
                   cls.options.user)):
            msg = 'Credentials insufficient, see help'
            raise SSHCredsNotFoundError(msg)

        if cls.juju_version is 'not_supported':
            raise JujuInstallationNotFoundError('juju not found/supported')
        elif cls.juju_version == 'juju1' or cls.juju_version == 'juju2':
            home = os.path.expanduser('~')
            if cls.juju_version == 'juju1':
                cls.pem_location = os.path.join(home, '.juju/ssh/juju_id_rsa')
            else:
                loc = '.local/share/juju/ssh/juju_id_rsa'
                cls.pem_location = os.path.join(home, loc)
            if not hasattr(cls, 'juju_ceph_machines'):
                cls.juju_ceph_machines = self._get_all_juju_ceph_machines()

        if cls.options.provider == 'ssh':
            if not hasattr(cls, 'is_juju'):
                cls.is_juju = False
            try:
                cls.connection = cls._get_connection(cls.options.host)
            except Exception as err:
                print err
                raise ConnectionFailedError('Couldnot connect to host')
        else:
            if not hasattr(cls, 'is_juju'):
                cls.is_juju = True
        if not hasattr(cls, 'init_type'):
            cls.init_type = self._get_init_type(cls.connection,
                                                cls.is_juju).strip()
        if cls.init_type == 'none':
            raise InitSystemNotSupportedError()
        if not hasattr(cls, 'arch_type'):
            cls.arch_type = self._get_arch_type(cls.connection,
                                                cls.is_juju).strip()

    def _get_machine_param(self, machine):
        id = machine['machine']
        public_addr = machine['public-address']
        cmd = 'juju run --machine ' + str(id) + ' "cat /etc/hostname"'
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        hostname = proc.communicate()[0].strip('\n')
        cmd = 'juju run --machine ' + str(id) + ' "host ' + hostname + ' "'
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        i_ip = proc.communicate()[0].strip('\n').split(' ')[-1]

        return id, public_addr, hostname, i_ip

    def _get_all_juju_ceph_machines(self):
        cls = TroubleshootCeph
        leader_id = sys.maxint
        cls.connection = None

        proc = subprocess.Popen('juju status --format json', shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            machine_list = json.loads(proc.communicate()[0])
        except ValueError:
            print 'Could not fetch machines from juju cli, aborting'
            exit()

        juju_machines = []
        for name, val in machine_list['services']['ceph']['units'].iteritems():
            jujuname = name
            try:
                id, public_addr, hostname, i_ip = self._get_machine_param(val)
            except ValueError:
                print 'Could not fetch details for ', val
            else:
                machine = JujuCephMachine(jujuname, id, public_addr, hostname,
                                          has_mon=True, internal_ip=i_ip)

                if int(id) < leader_id:
                    leader_id, cls.connection = int(id), machine

                juju_machines.append(machine)
                print 'Found - ', hostname, '-', jujuname, '-', public_addr,
                print '-', i_ip

        ceph_osd = machine_list['services']['ceph-osd']['units']
        for name, val in ceph_osd.iteritems():
            jujuname = name
            try:
                id, public_addr, hostname, i_ip = self._get_machine_param(val)
            except ValueError:
                print 'Could not fetch details for ', val
            else:
                machine = JujuCephMachine(jujuname, id, public_addr, hostname,
                                          has_osd=True, internal_ip=i_ip)
                juju_machines.append(machine)
                print 'Found - ', hostname, '-', jujuname, '-', public_addr,
                print '-', i_ip

        return juju_machines

    def _find_juju_version(self):
        proc = subprocess.Popen('juju --version', shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            stdout = proc.communicate()[0].strip('\n')
        except ValueError:
            print ' Could not get juju version, aborting'
            exit()

        if re.search(r'^2.*', stdout) is not None:
            return 'juju2'
        elif re.search(r'^1.*', stdout) is not None:
            return 'juju1'
        return 'not_supported'

    def _get_init_type(self, connection=None, juju=False):
        cmd = open(self.init_script, 'r').read()
        out, err = self._execute_command(connection, cmd, juju)
        return MyStr(out).read()

    def _get_arch_type(self, connection=None, juju=False):
        cmd = open(self.arch_script, 'r').read()
        out, err = self._execute_command(connection, cmd, juju)
        return MyStr(out).read()

    def _get_opt_parser(self):
        desc = ('Command line parser for CephDiagnoseTool \n'
                'Login method supported are: \n'
                'username + password + hostname, '
                'username + hostname + ssh_key_location, '
                'or use juju with provider flag')

        parser = optparse.OptionParser(description=desc)
        parser.add_option('-H', '--host', dest='host', default=None)
        parser.add_option('-u', '--user', dest='user', default='ubuntu')
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
    def _execute_juju_command(cls, connection, command):
            from base64 import b64encode
            command = '`echo ' + b64encode(command) + ' | base64 --decode`'
            cmd = 'juju run --machine ' + str(connection.id) + ' --timeout '
            cmd += str(cls.timeout) + 's' + ' "' + command + '"'
            out = subprocess.Popen(cmd, shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            return out.communicate()

    @classmethod
    def _execute_command(cls, connection, command, is_juju=False):
        if is_juju is True:
            (out, err) = cls._execute_juju_command(connection, command)
            if err.startswith('ERROR'):
                raise TimeoutError(command + ' timed out')
            else:
                return (out, err)
        else:
            (stdin, stdout, stderr) = connection.exec_command(command)
            return (stdout, stderr)

    @classmethod
    def _init_cli_down(cls):
        cls.options.ssh_key = cls.pem_location
        cls.is_juju = False
        cls.cli_down = True
        cls.connection = cls._get_connection(cls.connection.public_addr)

    def start_troubleshoot(self):
        cls = TroubleshootCeph
        command = 'sudo ceph health'
        cluster_status = None
        try:
            (output, err) = cls._execute_command(cls.connection, command,
                                                 is_juju=cls.is_juju)
            cls._get_eof(output, command)
            cluster_status = MyStr(output).read().split(' ')[0].strip()
        except TimeoutError as err:
            # ceph cli is not working i.e. quorum is not being established
            # hence we need to use ceph admin sockets
            print 'Please note that ceph cli is not working and you selected ',
            print 'juju as provider.'
            print 'We will use ssh(not juju) to connect to cluster.'
            print 'Currently this requires vanilla installation of juju(with ',
            print 'ssh keys are default location and user named ubuntu).'

            print '(yes/no) (default yes)?',
            response = raw_input()
            if response == 'yes' or response == '':
                print 'proceeding ',
                for i in range(20):
                    sys.stdout.write('.')
                print '\n'
                cls._init_cli_down()
            else:
                exit()
            return None
        return cluster_status

    @classmethod
    @timeout(10)
    def check_ceph_cli_health(cls, connection, command='sudo ceph health'):
        (output, err) = cls._execute_command(connection, command,
                                             is_juju=cls.is_juju)
        status = MyStr(output).read().split(' ')[0].strip()

        if status == 'HEALTH_OK':
            print 'Ceph cluster working again'
            exit()
        else:
            print "Didn't work, trying deeper probe"

    @classmethod
    @timeout(10)
    def _get_eof(cls, stream, command):
        if isinstance(stream, basestring):
            return
        while not stream.channel.eof_received:
            pass
        return stream.channel.eof_received

    @classmethod
    def poll_ceph_status(cls, connection, command='sudo ceph health'):
        tries = cls.options.timeout / 10
        status = None
        for i in range(tries):
            (out, err) = cls._execute_command(connection, command,
                                              is_juju=cls.is_juju)
            try:
                cls._get_eof(out, command)
            except TimeoutError:
                print 'retrying status'
            else:
                status = MyStr(out).read().split(' ')[0].strip()
                print status
                if status == 'HEALTH_OK':
                    return status
        return status
