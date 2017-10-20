import functools
import logging
import os
import os.path
import pickle
import time


def print_and_log(message, level='info', print_and_log=True):
    """
    A helper method that prints message to console and also writes to log with specified level
    :param message: str, the message
    :param level: str, the level of log: info, warn, error, critical
    :param print_and_log: bool. If False, logs only.
    :return: nothing
    """
    if print_and_log:
        print(message)
    if level == 'warn':
        logging.warning(message)
    elif level == 'error':
        logging.error(message)
    elif level == 'critical':
        logging.critical(message)
    else:
        logging.info(message)


def once_per_second(function, calls_per_second=1):
    """
    A decorator that sleeps for one second after executing the function. Used when scraping NHL site.

    This also means all functions that access the internet sleep for a second.
    :param function: the function
    :return: nothing
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        time.sleep(1 / calls_per_second)
        return function(*args, **kwargs)


def log_exceptions(function):
    """
    A decorator that wraps the passed in function and logs
    exceptions should one occur
    :param function: the function
    :return: nothing
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except:
            # log the exception
            err = "There was an exception in  "
            err += function.__name__
            logging.exception(err)

            # and write their args to file, named after function.
            index = 0  # used in case one function is called multiple times
            fname = get_logging_folder() + "{0:s}{1:d}.pkl".format(function.__name__, index)
            while os.path.exists(fname):
                index += 1
                fname = get_logging_folder() + "{0:s}{1:d}.pkl".format(function.__name__, index)

            f = open(fname, "w")
            pickle.dump(args, f)
            pickle.dump(kwargs, f)
            f.close()

            # f = open("example", "r")
            # value1 = pickle.load(f)
            # value2 = pickle.load(f)
            # f.close()

            # re-raise the exception
            raise

    return wrapper


def get_logging_folder():
    return './.logs/'


def start_logging():
    """Clears out logging folder, and starts the log in this folder"""

    if os.path.exists(get_logging_folder()):
        for file in os.listdir(get_logging_folder()):
            os.remove(get_logging_folder() + file)
    else:
        os.mkdir(get_logging_folder())

    logging.basicConfig(level=logging.DEBUG, filemode="w",
                        format="%(asctime)-15s %(levelname)-8s %(message)s",
                        filename=get_logging_folder() + 'logfile.log')

start_logging()
