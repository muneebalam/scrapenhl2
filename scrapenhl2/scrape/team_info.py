"""
This module contains methods related to the team info files.
"""

import json
import os.path
import urllib.error
import urllib.request

import feather
import pandas as pd

import scrapenhl2.scrape.organization as organization


def get_team_info_filename():
    """
    Returns the team information filename
    :return: /scrape/data/other/TEAM_INFO.feather
    """
    return os.path.join(organization.get_other_data_folder(), 'TEAM_INFO.feather')


def _get_team_info_file():
    """
    Returns the team information file. This is stored as a feather file for fast read/write.
    :return: file from /scrape/data/other/TEAM_INFO.feather
    """
    return feather.read_dataframe(get_team_info_filename())


def get_team_info_file():
    """
    Returns the team information file. This is stored as a feather file for fast read/write.
    :return: file from /scrape/data/other/TEAM_INFO.feather
    """
    return _TEAMS


def write_team_info_file(df):
    """
    Writes the team information file. This is stored as a feather file for fast read/write.
    :param df: the (team information) dataframe to write to file
    """
    feather.write_dataframe(df, get_team_info_filename())
    team_setup()


def get_team_info_url(teamid):
    """
    Gets the team url from the NHL API.
    :param teamid: int
        the team ID
    :return: str
        http://statsapi.web.nhl.com/api/v1/teams/[teamid]
    """
    return 'http://statsapi.web.nhl.com/api/v1/teams/{0:d}'.format(teamid)


def get_team_info_from_url(teamid):
    """
    Pulls ID, abbreviation, and name from the NHL API.
    :param teamid: int, the team ID
    :return: (id, abbrev, name)
    """

    teamid = int(teamid)
    url = get_team_info_url(teamid)
    with urllib.request.urlopen(url) as reader:
        page = reader.read()
    teaminfo = json.loads(page.decode('latin-1'))

    tid = teaminfo['teams'][0]['id']
    tabbrev = teaminfo['teams'][0]['abbreviation']
    tname = teaminfo['teams'][0]['name']

    return tid, tabbrev, tname


def add_team_to_info_file(teamid):
    """
    In case we come across teams that are not in the default list (1-110), use this method to add them to the file.
    :param teamid: int, the team ID
    :return: (tid, tabbrev, tname)
    """

    info = get_team_info_from_url(teamid)

    tid = info[0]
    tabbrev = info[1]
    tname = info[2]

    df = pd.DataFrame({'ID': [tid], 'Abbreviation': [tabbrev], 'Name': [tname]})
    teaminfo = pd.concat([df, get_team_info_file()])
    write_team_info_file(teaminfo)

    return info


def generate_team_ids_file(teamids=None):
    """
    Reads all team id URLs and stores information to disk. Has the following information:

    - ID: int
    - Abbreviation: str (three letters)
    - Name: str (full name)

    :param teamids: iterable of int. Tries to access team ids as listed in teamids. If not, goes from 1-110.
    :return: nothing
    """
    # TODO how about teams like 5460? Or Olympic teams? Read data automatically from game files in some cases
    # Maybe create a file with the list of teams
    # ed.print_and_log('Creating team IDs file', print_and_log=False)

    print('Creating team IDs file')

    ids = []
    abbrevs = []
    names = []

    default_limit = 110
    if teamids is None:
        # Read from current team ids file, if it exists
        try:
            teamids = set(get_team_info_file().ID.values)
        except:
            teamids = list(range(1, default_limit + 1))

    for i in teamids:
        try:
            tid, tabbrev, tname = get_team_info_from_url(i)

            ids.append(tid)
            abbrevs.append(tabbrev)
            names.append(tname)

            # ed.print_and_log('Done with ID # {0:d}: {1:s}'.format(tid, tname))
        except urllib.error.HTTPError:
            pass

    teaminfo = pd.DataFrame({'ID': ids, 'Abbreviation': abbrevs, 'Name': names})
    write_team_info_file(teaminfo)


def team_setup():
    """
    This method loads the team info df into memory
    :return: nothing
    """
    global _TEAMS
    _TEAMS = _get_team_info_file()


_TEAMS = None
team_setup()
