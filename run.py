import optparse
import paramiko

from helpers.exceptions import (SSHCredsNotFoundError, ConnectionFailedError,
                                TimeoutError)
from helpers.decorators import timeout


class TroubleshootCeph(object):
    '''
        TroubleshootCeph Class to be called to diagnose a ceph cluster
    '''
    GOOD_HEALTH = ['HEALTH_OK']
    BAD_HEALTH = ['HEALTH_WARN']

    def __init__(self):
        self.parser = self._get_opt_parser()
        self.options, self.arguments = self.parser.parse_args()

        if not (self.options.host and self.options.user):
            msg = 'Hostname and  Username are compulsary, see help'
            raise SSHCredsNotFoundError(msg)

        try:
            self.connection = self._get_connection(self.options.host,
                                                   self.options.user,
                                                   self.options.password)
        except Exception:
            raise ConnectionFailedError('Couldnot connect to host')

    def _get_opt_parser(self):
        desc = 'Command line parser for CephDiagnoseTool'

        parser = optparse.OptionParser(description=desc)
        parser.add_option('-H', '--host', dest='host', default=None)
        parser.add_option('-u', '--user', dest='user', default=None)
        parser.add_option('-p', '--pass', dest='password', default=None)
        return parser

    def _get_connection(self, hostname, username, password):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=hostname, username=username,
                       password=password)
        return client

    def _execute_command(self, connection, command):
        (stdin, stdout, stderr) = connection.exec_command(command)
        return (stdout, stderr)

    def troubleshoot_mon(self):
        command = 'sudo ceph health'
        (output, err) = self._execute_command(self.connection, command)

        try:
            self._get_eof(output, command)
            cluster_status = output.read().split(' ')[0].strip()

        except TimeoutError as err:
            # cluster is screwed
            # Best guess some mon machines are down
            # just printing error for now
            # TODO
            print err
            return

        if cluster_status in self.GOOD_HEALTH:
            print cluster_status
            print 'All good up here, no mon issues :-)'

        if cluster_status in self.BAD_HEALTH:
            print 'MON Status : ', cluster_status
            print '\nProbable cause Ceph service not running in some machines'
            print 'Try & start service in the machines not in quorum',
            print '(yes/no) (default no)?',
            response = raw_input()

            if response in ['no', '']:
                print 'not proceeding with updating machines, aborting'
                return
            if response not in ['yes']:
                print 'response not valid, aborting'
                return

            command = 'sudo ceph mon_status'
            (output, err) = self._execute_command(self.connection, command)

            mon_status = eval(output.read())
            quorum_list = mon_status['quorum']
            mon_list = mon_status['monmap']['mons']

            if len(quorum_list) != len(mon_list):
                for mon in mon_list:
                    if mon['rank'] not in quorum_list:
                        print '\n' + mon['name'] + ' not in quorum list,',
                        print 'restarting ceph services'
                        mon_addr = mon['addr'].split(':')[0]

                        self._restart_ceph_services(mon_addr,
                                                    self.options.user,
                                                    self.options.password)

    def _restart_ceph_services(self, addr, username, password):
        connection = self._get_connection(addr, username, password)
        self._execute_command(connection, 'sudo start ceph-all')
        print addr + 'Restart successful'

    @timeout(10)
    def _get_eof(self, stream, command=''):
        while not stream.channel.eof_received:
            pass
        return stream.channel.eof_received

if __name__ == "__main__":
    TroubleshootCeph = TroubleshootCeph()
    TroubleshootCeph.troubleshoot_mon()
