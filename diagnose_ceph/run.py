from helpers.exceptions import QuorumIssueNotResolvedError
from troubleshoot_ceph import TroubleshootCeph
from troubleshoot_ceph_mon import TroubleshootCephMon
from troubleshoot_ceph_osd import TroubleshootCephOsd


def run():
    TroubleshootCeph_ = TroubleshootCeph()
    cluster_status = TroubleshootCeph_.start_troubleshoot()
    if cluster_status == 'HEALTH_OK':
        print 'All good with monitors up here :-)'
    elif cluster_status is None:
        print 'No Cluster Status Returned'
        TroubleshootCephMon_ = TroubleshootCephMon(is_ceph_cli=False)
        TroubleshootCephMon_.troubleshoot_mon()
    else:
        print cluster_status
        TroubleshootCephMon_ = TroubleshootCephMon(is_ceph_cli=True)
        TroubleshootCephMon_.troubleshoot_mon()

    # If the script reaches here we check for osd issues
    # First lets check if the ceph cli is working
    cluster_status = TroubleshootCeph_.start_troubleshoot()
    if cluster_status is None:
        msg = 'ceph cli could not work, can not proceed'
        raise QuorumIssueNotResolvedError(msg)

    TroubleshootCephOsd_ = TroubleshootCephOsd()
    TroubleshootCephOsd_.troubleshoot_osd()
