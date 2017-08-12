
class TimeoutException(Exception):
    msg = "Timeout has been occured."

    def __init__(self, message=None, **kwargs):
        if not message:
            message = self.msg
        super(TimeoutException, self).__init__(message, kwargs)


class NotSupportException(Exception):
    msg = "Does Not Suupport."

    def __init__(self, message=None, **kwargs):
        if not message:
            message = self.msg
        super(NotSupportException, self).__init__(message, kwargs)


class ExceedQuotaException(Exception):
    msg = "Exceed backup quota"

    def __int__(self, message=None, **kwargs):
        if not message:
            message = self.msg
        super(ExceedQuotaException, self).__init__(message, kwargs)

class FailedBackupException(Exception):
    msg = "Failed backup"

    def __int__(self, message=None, **kwargs):
        if not message:
            message = self.msg
        super(FailedBackupException, self).__init__(message, kwargs)

class FailedAdminException(Exception):
    msg = "Failed admin"

    def __int__(self, message=None, **kwargs):
        if not message:
            message = self.msg
        super(FailedAdminException, self).__init__(message, kwargs)

class FailedRestoreException(Exception):
    msg = "Failed restore"

    def __int__(self, message=None, **kwargs):
        if not message:
            message = self.msg
        super(FailedRestoreException, self).__init__(message, kwargs)
