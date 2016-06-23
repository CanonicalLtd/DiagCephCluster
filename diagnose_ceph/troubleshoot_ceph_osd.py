import json

from helpers.exceptions import ConnectionFailedError, TimeoutError
from troubleshoot_ceph import TroubleshootCeph


class OsdObject(object):
    def __init__(self, host, name, id, ssh_status, status, in_cluster,
                 connection=None):
        self.name = name
        self.id = id
        self.host = host
        self.ssh_status = ssh_status
        self.connection = connection
        self.status = status
        self.in_cluster = in_cluster


class TroubleshootCephOsd(TroubleshootCeph):
    def troubleshoot_osd(self):
        status = self._poll_osd_status(self.connection)
        print status
        if status == 'OSD_FULL':
            print 'OSD objects almost full'
            return
        elif status == 'OSD_OK':
            print 'No OSD Issues :-)'
            return
        elif status is None:
            print 'ceph cli down can not proceed'
            return

        self.osd_objects = self._get_all_osd_object()

        # Let's try restarting osd daemons that are down
        print 'trying to restart osd daemon that are down'
        self._restart_dead_osd()

    def _restart_dead_osd(self):
        for osd in self.osd_objects:
            if osd.ssh_status and osd.status == 'down':
                self._restart_osd(osd)

    def _restart_osd(self, osd, cmd='start'):
        if self.init_type in ['upstart', 'sysv-init']:
            cmd = 'sudo ' + cmd + ' ceph-osd id=' + str(osd.id)
        else:
            cmd = 'sudo systemctl ' + cmd + ' ceph-osd@' + str(osd.id) +\
                '.service'
        out, err = self._execute_command(osd.connection, cmd)
        print osd.name + ': ' + cmd + ' successful'

    def _get_all_osd_object(self):
        osd_objects = []
        osd_list = self._get_osd_list()
        out, err = self._execute_command(self.connection,
                                         'sudo ceph osd tree --format=json')
        osd_tree = json.loads(out.read())

        for node in osd_tree['nodes']:
            if node['name'] in osd_list:
                status = node['status']
                in_cluster = 'out' if node['reweight'] == 0.0 else 'in'
                host = self._get_osd_details(node['id'])

                try:
                    connection = self._get_connection(host)
                except ConnectionFailedError as err:
                    osd_obj = OsdObject(host, node['name'], node['id'], False,
                                        status, in_cluster)
                else:
                    osd_obj = OsdObject(host, node['name'], node['id'], True,
                                        status, in_cluster, connection)

                    osd_objects.append(osd_obj)

        return osd_objects

    def _get_osd_details(self, osd_id):
        cmd = 'sudo ceph osd find ' + str(osd_id) + ' --format json'
        out, err = self._execute_command(self.connection, cmd)
        details = json.loads(out.read())
        return details['crush_location']['host']

    def _get_osd_list(self):
        out, err = self._execute_command(self.connection, 'sudo ceph osd ls')
        return map(lambda x: 'osd.' + str(x), filter(lambda x: x is not '',
                                                     out.read().split('\n')))

    def _poll_osd_status(self, connection):
        tries = self.options.timeout / 10
        status = None
        cmd = 'sudo ceph osd stat --format json'
        for i in range(tries):
            (out, err) = self._execute_command(connection, cmd)
            try:
                self._get_eof(out, cmd)
            except TimeoutError:
                print 'retrying status'
            else:
                status = json.loads(out.read())
                if status['full'] or status['nearfull']:
                    return 'OSD_FULL'
                elif (status['num_osds'] == status['num_up_osds'] and
                      status['num_up_osds'] == status['num_in_osds']):
                    return 'OSD_OK'
                else:
                    return 'OSD_NOT_OK'
        return status
