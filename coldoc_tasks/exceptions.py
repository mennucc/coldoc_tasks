
class ColdocTasksException(Exception):
    "Generic base class for exceptions in this module"

class ColdocTasksProcessLookupError(ColdocTasksException):
    """ Process not found. """

try:
    import celery
    import celery.exceptions
    class ColdocTasksTimeoutError(celery.exceptions.TimeoutError, ColdocTasksException):
        """ Timeout expired. """
    #
except ImportError:
    #
    class ColdocTasksTimeoutError(ColdocTasksException):
        """ Timeout expired. """
