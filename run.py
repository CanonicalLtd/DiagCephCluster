import paramiko
import optparse

from helpers.exceptions import SSHCredsNotFoundError, ConnectionFailedError


class TroubleshootCeph(object):
    '''
        TroubleshootCeph Class to be called to diagnose a ceph cluster
    '''
    GOOD_HEALTH = ('HEALTH_OK')
    BAD_HEALTH = ('HEALTH_WARN')

    def __init__(self):
        self.parser = self._get_opt_parser()
        self.options, self.arguments = self.parser.parse_args()

        if not (self.options.host and self.options.user):
            msg = 'Hostname and  Username are compulsary, see help'
            raise SSHCredsNotFoundError(msg)

        try:
            self.connection = self._get_connection()
        except Exception:
            raise ConnectionFailedError('Couldnot connect to host')

    def _get_opt_parser(self):
        desc = 'Command line parser for CephDiagnoseTool'

        parser = optparse.OptionParser(description=desc)
        parser.add_option('-H', '--host', dest='host', default=None)
        parser.add_option('-u', '--user', dest='user', default=None)
        parser.add_option('-p', '--pass', dest='password', default=None)
        return parser

    def _get_connection(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=self.options.host, username=self.options.user,
                       password=self.options.password)
        return client

    def _execute_command(self, command):
        (stdin, stdout, stderr) = self.connection.exec_command(command)
        return (stdout, stderr)


if __name__ == "__main__":
    TroubleshootCeph = TroubleshootCeph()
