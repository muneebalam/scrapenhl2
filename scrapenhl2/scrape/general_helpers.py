"""
This module contains general helper methods. None of these methods have dependencies on other scrapenhl2 modules.
"""

import functools
import logging
import os
import os.path
import pickle
import re
import time
import urllib.request
import urllib.error

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz


def print_and_log(message, level='info', print_and_log=True):
    """
    A helper method that prints message to console and also writes to log with specified level.

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


def once_per_second(fn, calls_per_second=1):
    """
    A decorator that sleeps for one second after executing the function. Used when scraping NHL site.
    This also means all functions that access the internet sleep for a second.

    :param fn: the function

    :return: nothing
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        time.sleep(1 / calls_per_second)
        return fn(*args, **kwargs)


def log_exceptions(fn):
    """
    A decorator that wraps the passed in function and logs exceptions should one occur

    :param function: the function

    :return: nothing
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except:
            # log the exception
            err = "There was an exception in  "
            err += fn.__name__
            logging.exception(err)

            # and write their args to file, named after function.
            index = 0  # used in case one function is called multiple times
            fname = get_logging_folder() + "{0:s}{1:d}.pkl".format(fn.__name__, index)
            while os.path.exists(fname):
                index += 1
                fname = get_logging_folder() + "{0:s}{1:d}.pkl".format(fn.__name__, index)

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


def check_types(obj):
    """
    A helper method to check if obj is int, float, np.int64, or str. This is frequently needed, so is helpful.

    :param obj: the object to check the type

    :return: bool
    """
    return check_number(obj) or isinstance(obj, str)


def check_number(obj):
    """
    A helper method to check if obj is int, float, np.int64, etc. This is frequently needed, so is helpful.

    :param obj: the object to check the type

    :return: bool
    """
    return isinstance(obj, int) or isinstance(obj, float) or isinstance(obj, np.number)


def check_number_last_first_format(name):
    """
    Checks if specified name looks like "8 Ovechkin, Alex"

    :param name: str

    :return: bool
    """
    if re.match('^\d{1,2}\s*[A-Z]+\s*[A-Z]+', name.replace("'", '')) is None:  # added in apostrophe case for O'Brien
        return False
    return True


@functools.lru_cache(maxsize=128, typed=False)
def infer_season_from_date(date):
    """
    Looks at a date and infers the season based on that: Year-1 if month is Aug or before; returns year otherwise.

    :param date: str, YYYY-MM-DD

    :return: int, the season. 2007-08 would be 2007.
    """
    season, month, day = [int(x) for x in date.split('-')]
    if month < 9:
        season -= 1
    return season


def mmss_to_secs(strtime):
    """
    Converts time from mm:ss to seconds

    :param strtime: str, mm:ss

    :return: int
    """
    mins, sec = strtime.split(':')
    return 60 * int(mins) + int(sec)


def try_to_access_dict(base_dct, *keys, **kwargs):
    """
    A helper method that accesses base_dct using keys, one-by-one. Returns None if a key does not exist.

    :param base_dct: dict, a dictionary
    :param keys: str, int, or other valid dict keys
    :param kwargs: can specify default using kwarg default_return=0, for example.

    :return: obj, base_dct[key1][key2][key3]... or None if a key is not in the dictionary
    """
    temp = base_dct
    default_return = None
    for k, v in kwargs.items():
        default_return = v

    try:
        for key in keys:
            temp = temp[key]
        return temp
    except KeyError:  # for string keys
        return default_return
    except IndexError:  # for array indices
        return default_return
    except TypeError:  # might not be a dictionary or list
        return default_return


def add_sim_scores(df, name):
    """
    Adds fuzzywuzzy's token set similarity scores to provded dataframe

    :param df: pandas dataframe with column Name
    :param name: str, name to compare to

    :return: df with an additional column SimScore
    """
    df.loc[:, 'SimScore'] = df.Name.apply(lambda x: fuzz.token_set_ratio(name, x))
    return df


def fuzzy_match_player(name_provided, names, minimum_similarity=50):
    """
    This method checks similarity between each entry in names and the name_provided using token set matching and
    returns the entry that matches best. Returns None if no similarity is greater than minimum_similarity.
    (See e.g. http://chairnerd.seatgeek.com/fuzzywuzzy-fuzzy-string-matching-in-python/)

    :param name_provided: str, name to look for
    :param names: list (or ndarray, or similar) of
    :param minimum_similarity: int from 0 to 100, minimum similarity. If all are below this, returns None.

    :return: str, string in names that best matches name_provided
    """
    df = pd.DataFrame({'Name': names})
    df = add_sim_scores(df, name_provided)
    df = df.sort_values(by='SimScore', ascending=False).query('SimScore >= {0:f}'.format(minimum_similarity))
    if len(df) == 0:
        print('Could not find match for {0:s}'.format(name_provided))
        return None
    else:
        # print(df.iloc[0])
        return df.Name.iloc[0]


def intervals(lst, interval_pct=10):
    """
    A method that divides list into intervals and returns tuples indicating each interval mark.
    Useful for giving updates when cycling through games.

    :param lst: lst to divide
    :param interval_pct: int, pct for each interval to represent. e.g. 10 means it will mark every 10%.

    :return: a list of tuples of (index, value)
    """

    lst = sorted(lst)
    dfintervals = []
    i = 0
    while True:
        frac = interval_pct / 100 * i
        index = round(len(lst) * frac)
        if index >= len(lst):
            break
        val = lst[index]
        dfintervals.append((index, val))
        i += 1
    return dfintervals


def remove_leading_number(string):
    """
    Will convert 8 Alex Ovechkin to Alex Ovechkin, or Alex Ovechkin to Alex Ovechkin

    :param string: a string

    :return: string without leading numbers
    """
    newstring = string
    while newstring[0] in {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}:
        newstring = newstring[1:]
    return newstring.strip()


def flip_first_last(name):
    """
    Changes Ovechkin, Alex to Alex Ovechkin. Also changes to title case.

    :param name: str

    :return: str, flipped if applicable
    """
    if ',' not in name:
        return name

    # What about case of , Jr or , IV? Ignore for now
    newname = ' '.join([x.strip() for x in name.split(',')[::-1]])
    return newname.title()


def period_contribution(x):
    """
    Turns period--1, 2, 3, OT, etc--into # of seconds elapsed in game until start.
    :param x: str or int, 1, 2, 3, etc
    :return: int, number of seconds elapsed until start of specified period
    """
    try:
        x = int(x)
        return 1200 * (x - 1)
    except ValueError:
        return 3600 if x == 'OT' else 3900  # OT or SO


def get_lastname(pname):
    """
    Splits name on first space and returns second part.

    :param pname: str, player name

    :return: str, player last name
    """
    return pname.split(' ', 1)[1]


def get_initials(pname):
    """
    Splits name on spaces and returns first letter from each part.

    :param pname: str, player name

    :return: str, player initials
    """
    return ''.join([part[0] for part in pname.split(' ')])


def try_url_n_times(url, timeout=5, n=5):
    """
    A helper method that tries to access given url up to five times, returning the page.

    :param url: str, the url to access
    :param timeout: int, number of secs to wait before timeout. Default 5.
    :param n: int, the max number of tries. Default 5.

    :return: bytes
    """

    page = None
    tries = 0
    while tries < n:
        tries += 1
        try:
            with urllib.request.urlopen(url, timeout=5) as reader:
                page = reader.read()
            break
        except urllib.error.HTTPError as httpe:
            if '404' in str(httpe):
                break
            else:
                print('HTTP error with', url, httpe, httpe.args)
        except Exception as e:  # timeout
            tries += 1
            print('Could not access {0:s}; try {1:d} of {2:d}'.format(url, tries, n))
    return page
