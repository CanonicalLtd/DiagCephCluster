from ConfigParser import ConfigParser
import optparse
import os
import paramiko
import re
import time

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
        elif status == 'HEALTH_WARN':
            print "Didn't work, trying deeper probe"

    @classmethod
    @timeout(20)
    def _get_eof(cls, stream, command):
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
            print 'ceph cli not working'
            self._troubleshoot_mon_no_cli()

    def _troubleshoot_mon_no_cli(self):
        '''
            Troubleshoot mon issues using monitor sockets when ceph cli
            doesnt work i.e. quorum is not being established
        '''
        # Lets store all the machine list by parsing the /etc/ceph.conf
        self.machines = self._get_machine_objects()

        # Starting checklist when ceph cli is down
        # first we restart all machines as don't have many options here

        print '\nProbable cause Ceph mon service not running in some machines'
        print 'Try & start the mon service in every machine',
        print '(yes/no) (default no)?',
        response = raw_input()

        if response != 'yes':
            print 'not proceeding with starting machines, aborting'
            return

        self._restart_all_machines()

        time.sleep(10)
        try:
            self.check_ceph_cli_health(self.connection)
        except TimeoutError:
            print "Restarting servers not in quorum didn't work,"
            print 'trying deeper probe'

        # Now let's call the common check list
        self._troubleshoot_mon_common()

    def _restart_all_machines(self):
        for machine in self.machines:
            if machine.ssh_status == 'LIVE':
                self._restart_ceph_mon_service('start', machine.host)

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

        # Let's wait for sometime before getting ceph health status
        time.sleep(10)
        try:
            self.check_ceph_cli_health(self.connection)
        except TimeoutError:
            print "Restarting servers not in quorum didn't work,"
            print 'trying deeper probe'

        # Now let's call the common checklist
        self._troubleshoot_mon_common()

    def _troubleshoot_mon_common(self):
        ''' Common checklist for both when ceph cli is working or not '''

        # Lets store all the machine list for future use
        self.machines = self._get_machine_objects()

        print 'Trying to detect for Clock Skew'
        status = None
        try:
            status = self._detect_clock_skew(self.connection)
        except TimeoutError:
            print 'ceph health command did not work proceeding to next phase'

        if status is None:
            print 'Could not detect any Clock Skew, proceeding to deeper probe'
        else:
            print 'Clock Skew detected for', status, 'Try start ntp server?'
            print 'We assume ntpd is installed here'
            print '(yes/no) (default no)?',
            response = raw_input()

            if response != 'yes':
                print 'aborting'
                exit()
            else:
                self._correct_skew(status)

        # Let's wait for sometime before getting ceph health status
        time.sleep(10)
        try:
            self.check_ceph_cli_health(self.connection)
        except TimeoutError:
            print "Restarting ntpd did not work, probing deeper"

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

        # Let's wait for sometime before getting ceph health status
        time.sleep(10)
        try:
            self.check_ceph_cli_health(self.connection)
        except TimeoutError:
            print "Injecting Monmap didn't work, probably network issue"

    def _correct_skew(self, skew_list):
        for mon in skew_list:
            for machine in self.machines:
                if mon == machine.mon_id:
                    if self.init_type == 'systemd':
                        cmd = 'sudo systemctl restart ntp.service'
                    else:
                        cmd = 'sudo service ntp restart'
                    out, err = self._execute_command(machine.connection, cmd)
                    try:
                        self._get_eof(out, cmd)
                    except TimeoutError:
                        print "Couldn't restart ntp for: ", mon
                    else:
                        print 'Restart successful for: ', mon

    def _detect_clock_skew(self, connection):
        out, err = self._execute_command(connection, 'sudo ceph health')

        try:
            self._get_eof(out, 'sudo ceph health')
        except TimeoutError:
            raise TimeoutError('sudo ceph health timed out')

        status = out.read().split(' ', 1)

        if not (len(status) == 2 and status[0].strip() in self.BAD_HEALTH):
            return None

        if re.search(r'clock skew detected on.*', status[1]) is None:
            return None

        skew_list = [i.strip()[4:] for i in
                     re.findall(r'(mon\..*)[;]', status[1])[0].split(',')]
        return skew_list

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
                    loc = '/tmp/monmap'
                    if correct_mon_host is None:
                        try:
                            self._save_monmap(machine, loc)
                        except TimeoutError:
                            pass
                        else:
                            correct_mon_host = machine
                    machine.is_monmap_correct = True

        if correct_mon_host is None:
            return None
        return loc

    def _save_monmap(self, mon_host, loc):
        '''
            Try to save monmap of a mon node which has the correct one
            There maybe scenarios when ceph mon getmap may take alot of
            may timeout. In that case raise TimeoutError and try next
            correct node
        '''
        self._restart_ceph_mon_service('stop', mon_host.host)

        cmd = 'sudo ceph mon getmap -o /tmp/monmap'
        out, err = self._execute_command(mon_host.connection, cmd)
        try:
            self._get_eof(out, cmd)
        except TimeoutError:
            raise TimeoutError('ceph mon getmap timed out')
        mon_host.connection.open_sftp().get('/tmp/monmap', loc)
        self._restart_ceph_mon_service('start', mon_host.host)

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
        print addr + ': ' + cmd + ' successful'


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
