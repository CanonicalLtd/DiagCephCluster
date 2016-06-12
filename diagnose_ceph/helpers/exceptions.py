class SSHCredsNotFoundError(Exception):
    def __init__(self, message):
        super(SSHCredsNotFoundError, self).__init__(message)


class ConnectionFailedError(Exception):
    def __init__(self, message):
        super(ConnectionFailedError, self).__init__(message)


class TimeoutError(Exception):
    def __init__(self, message):
        super(TimeoutError, self).__init__(message)


class InitSystemNotSupportedError(Exception):
    def __init__(self, message):
        super(InitSystemNotSupportedError, self).__init__(message)


class QuorumIssueNotResolvedError(Exception):
    def __init__(self, message):
        super(QuorumIssueNotResolvedError, self).__init__(message)
