from ConfigParser import ConfigParser
import os
import subprocess
import re

from helpers.exceptions import ConnectionFailedError, TimeoutError
from helpers.helpers import MyStr
from troubleshoot_ceph import TroubleshootCeph


class MonObject(object):
    """ Constructs an object with all attributes of a ceph-mon machine.

        Note:
            To construct an object when ssh is to be used use __init__().
            To construct an object when juju cli is used use juju_machine().

        Args:
            host (str): ip address of the machine.
            ssh_status (bool): ssh_status of the machine(True/False).
            connection (:obj:`paramiko_conn_obj/MonObject_instance`):
                In case of ssh enabled diagnosis connection is paramiko
                connection object. In case of juju cli diagnosis connection
                is an instance of the MonObject itself as that is used for
                establishing connection(refer _execute_command in
                TroubleshootCeph class).
            admin_socket (str): admin_socket daemon running on the ceph machine
                This is only in working state when daemon is running else None.
            **kwargs (dict): Additional parameters required when object is
                initialized using juju_machine method. Juju cli requires an
                additional id attribute and juju machine_name attribute.
    """

    def __init__(self, host, ssh_status, connection=None, admin_socket=None,
                 **kwargs):
        self.host = host
        self.admin_socket = admin_socket
        self.connection = connection
        self.ssh_status = ssh_status
        self.mon_id = None if admin_socket is None else admin_socket[9:-5]
        self.is_monmap_correct = False
        if 'juju_id' in kwargs:
            self.connection = self
        if 'hostname' in kwargs:
            self.mon_id = kwargs['hostname']
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

    @classmethod
    def juju_machine(cls, public_addr, admin_socket, hostname, juju_name,
                     juju_id):
        return cls(host=public_addr, ssh_status='LIVE', connection=None,
                   admin_socket=admin_socket, juju_id=juju_id,
                   juju_name=juju_name, hostname=hostname)

    @property
    def id(self):
        return self.juju_id


class TroubleshootCephMon(TroubleshootCeph):
    def __init__(self, is_ceph_cli):
        self.is_ceph_cli = is_ceph_cli

    def troubleshoot_mon(self):
        '''
            Based on ceph mon quorum status  troubleshoot using either
            ceph cli or ceph mon admin sockets
        '''
        # Let's store all the machines for future use
        if self.is_juju is True:
            self.machines = self._get_juju_machine_objects()
        else:
            self.machines = self._get_machine_objects()

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
        # Starting checklist when ceph cli is down
        # First let's call the common check list
        self._troubleshoot_mon_common()

        # Didnt work try restart all mon machines

        print '\nProbable cause Ceph mon service not running in some machines'
        print 'Try & start the mon service in every machine',
        print '(yes/no) (default no)?',
        response = raw_input()

        if response != 'yes':
            print 'not proceeding with starting machines, aborting'
            return

        self._restart_all_mon_daemons()

        if self.poll_ceph_status(self.connection) == 'HEALTH_OK':
            print 'Ceph Cluster working again :-)'
            exit()
        else:
            print "Restarting all mon servers didn't work,"

    def _restart_all_mon_daemons(self):
        for machine in self.machines:
            if machine.ssh_status == 'LIVE':
                print 'Restarting machine ' + machine.host
                self._restart_ceph_mon_service('start', machine.connection)

    def _troubleshoot_mon_cli(self):
        '''
            Troubleshoot mon issues using ceph cli i.e. the quorum has been
            established
        '''
        print '\nProbable cause Ceph mon service not running in some machines'
        print 'Try & start service in the machines not in quorum',
        print '(yes/no) (default no)?',
        response = raw_input()

        if response != 'yes':
            print 'not proceeding with updating machines, aborting'
            return

        # Start ceph cli check_list
        self._restart_dead_mon_daemons()

        if self.poll_ceph_status(self.connection) == 'HEALTH_OK':
            print 'Ceph Cluster working again :-)'
            exit()
        else:
            print "Restarting servers not in quorum didn't work,"
            print 'Trying deeper probe'

        # Now let's call the common checklist
        self._troubleshoot_mon_common()

    def _troubleshoot_mon_common(self):
        ''' Common checklist for both when ceph cli is working or not '''
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

        if self.poll_ceph_status(self.connection) == 'HEALTH_OK':
            print 'Ceph Cluster working again :-)'
            exit()
        else:
            print "Restarting ntpd didn't work, probing deeper"

        if self.advance:  # if the user desires advance checks.
            print 'Inject correct monmap to machines with incorrect monmap?'
            print '(yes/no) (default no)?',
            response = raw_input()

            if response != 'yes':
                print 'not proceeding with updating machines, aborting'
                return

            monmap_loc = self._find_correct_monmap(self.machines)
            if monmap_loc is None:
                print 'No Mon has correct monmap, recovery impossible, abort'
                return

            self._inject_mon_map(monmap_loc, self.machines)

            if self.poll_ceph_status(self.connection) == 'HEALTH_OK':
                print 'Ceph Cluster working again :-)'
                exit()
            else:
                print "Injecting Monmap didn't work, probably Network issue"

    def _correct_skew(self, skew_list):
        for mon in skew_list:
            for machine in self.machines:
                if mon == machine.mon_id:
                    if self.init_type == 'systemd':
                        cmd = 'sudo systemctl restart ntp.service'
                    else:
                        cmd = 'sudo service ntp restart'
                    out, err = self._execute_command(machine.connection, cmd,
                                                     self.is_juju)
                    try:
                        self._get_eof(out, cmd)
                    except TimeoutError:
                        print "Couldn't restart ntp for: ", mon
                    else:
                        print 'Restart successful for: ', mon

    def _detect_clock_skew(self, connection):
        out, err = self._execute_command(connection, 'sudo ceph health',
                                         self.is_juju)

        try:
            self._get_eof(out, 'sudo ceph health')
        except TimeoutError:
            raise TimeoutError('sudo ceph health timed out')

        status = MyStr(out).read().split(' ', 1)
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
                self._restart_ceph_mon_service('stop', machine.connection)

                if self.is_juju:
                    cmd = 'juju scp ' + monmap_loc + ' ' + str(machine.id) +\
                        ':/tmp/monmap'
                    subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
                else:
                    machine.connection.open_sftp().put(monmap_loc,
                                                       '/tmp/monmap')

                cmd = 'sudo ceph-mon -i ' + machine.mon_id +\
                    ' --inject-monmap /tmp/monmap'
                self._execute_command(machine.connection, cmd, self.is_juju)

                self._restart_ceph_mon_service('start', machine.connection)

    def _find_correct_monmap(self, machine_list):
        mon_host_id = []
        for machine in machine_list:
            mon_host_id.append(machine.mon_id)
        mon_host_id.sort()
        correct_mon_host = None
        for machine in machine_list:
            if (machine.ssh_status == 'LIVE' and
               machine.admin_socket is not None):

                cmd = 'sudo ceph --admin-daemon /var/run/ceph/' +\
                    machine.admin_socket + ' mon_status'
                out, err = self._execute_command(machine.connection, cmd,
                                                 self.is_juju)
                monmap = eval(MyStr(out).read())['monmap']['mons']
                monmap_host_id = sorted([i['name'] for i in monmap])
                if monmap_host_id == mon_host_id:
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
        self._restart_ceph_mon_service('stop', mon_host.connection)
        cmd = 'sudo ceph-mon -i ' + mon_host.mon_id + ' --extract-monmap ' +\
            '/tmp/monmap'
        out, err = self._execute_command(mon_host.connection, cmd,
                                         self.is_juju)

        try:
            self._get_eof(out, cmd)
        except TimeoutError:
            raise TimeoutError('ceph mon getmap timed out')
            cmd = 'juju scp ' + str(mon_host.id) + ':/tmp/monmap ' + loc
            if self.is_juju:
                subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            else:
                mon_host.connection.open_sftp().get('/tmp/monmap', loc)

        self._restart_ceph_mon_service('start', mon_host.connection)

    def _get_juju_machine_objects(self):
        # Here we assume all ceph/* have a mon service
        machines = []
        for machine in self.juju_ceph_machines:
            if machine.has_mon is True:
                out, err = self._execute_command(machine, 'ls /var/run/ceph',
                                                 is_juju=True)
                socket = self._find_mon_socket(MyStr(out).read().split('\n'))
                machine = MonObject.juju_machine(machine.public_addr, socket,
                                                 machine.hostname,
                                                 machine.name, machine.id)
                machines.append(machine)
        return machines

    def _get_machine_objects(self):
        mon_list = self._get_mon_list()
        machines = []

        for i in mon_list:
            addr = i
            if self.cli_down:
                for m in self.juju_ceph_machines:
                    if m.internal_ip == i:
                        addr = m.public_addr
            try:
                connection = self._get_connection(addr)
            except ConnectionFailedError as err:
                dead_mon = MonObject(addr, 'DEAD')
                machines.append(dead_mon)
            else:
                out, err = self._execute_command(connection,
                                                 'ls /var/run/ceph/',
                                                 self.is_juju)
                socket = self._find_mon_socket(out.read().split('\n'))
                live_mon = MonObject(addr, 'LIVE', connection, socket)
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
                                              'sudo ceph mon_status',
                                              self.is_juju)

        mon_status = eval(MyStr(output).read())
        quorum_list = mon_status['quorum']
        mon_list = mon_status['monmap']['mons']

        if len(quorum_list) != len(mon_list):
            for mon in mon_list:
                if mon['rank'] not in quorum_list:
                    print '\n' + mon['name'] + ' not in quorum list,',
                    print 'restarting ceph services'
                    if not self.is_juju:
                        mon_addr = mon['addr'].split(':')[0]
                        connection = self._get_connection(mon_addr)
                    else:
                        for machine in self.machines:
                            if machine.mon_id == mon['name']:
                                connection = machine
                                break
                    self._restart_ceph_mon_service('start', connection)

    def _restart_ceph_mon_service(self, cmd, connection):
        if self.init_type in ['upstart', 'sysv-init']:
            cmd = 'sudo ' + cmd + ' ceph-mon-all'
        else:
            cmd = 'sudo systemctl ' + cmd + ' ceph-mon.service'
        self._execute_command(connection, cmd, self.is_juju)
