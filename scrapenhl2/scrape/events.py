"""
This module contains methods related to PBP events.
"""

import functools


def convert_event(event):
    """
    Converts to a more convenient, standardized name (see get_event_dictionary)

    :param event: str, the event name

    :return: str, shortened event name
    """
    return get_event_dictionary()[event]  # TODO the event dictionary is missing some


def _get_event_dictionary():
    """
    Runs at startup to get a mapping of event name abbreviations to long versions.

    :return: a dictionary mapping, e.g., 'fo' to 'faceoff'. All lowercase.
    """
    return {'fac': 'faceoff', 'faceoff': 'faceoff',
            'shot': 'shot', 'sog': 'shot', 'save': 'shot',
            'hit': 'hit',
            'stop': 'stoppage', 'stoppage': 'stoppage',
            'block': 'blocked shot', 'blocked shot': 'blocked shot',
            'miss': 'missed shot', 'missed shot': 'missed shot',
            'giveaway': 'giveaway', 'give': 'giveaway',
            'takeaway': 'take', 'take': 'takeaway',
            'penl': 'penalty', 'penalty': 'penalty',
            'goal': 'goal',
            'period end': 'period end',
            'period official': 'period official',
            'period ready': 'period ready',
            'period start': 'period start',
            'game scheduled': 'game scheduled',
            'gend': 'game end',
            'game end': 'game end',
            'shootout complete': 'shootout complete',
            'chal': 'official challenge', 'official challenge': 'official challenge'}


def get_event_dictionary():
    """
    Returns the abbreviation: long name event mapping (in lowercase)

    :return: dict of str:str
    """
    return _EVENT_DICT


@functools.lru_cache(maxsize=10, typed=False)
def get_event_longname(eventname):
    """
    A method for translating event abbreviations to full names (for pbp matching)

    :param eventname: str, the event name

    :return: the non-abbreviated event name
    """
    return get_event_dictionary()[eventname]


def event_setup():
    """
    Loads event dictionary into memory

    :return: nothing
    """
    global _EVENT_DICT
    _EVENT_DICT = _get_event_dictionary()


event_setup()
