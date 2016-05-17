class SSHCredsNotFoundError(Exception):
    def __init__(self, message):
        super(SSHCredsNotFoundError, self).__init__(message)


class ConnectionFailedError(Exception):
    def __init__(self, message):
        super(ConnectionFailedError, self).__init__(message)
