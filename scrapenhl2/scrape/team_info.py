"""
This module contains methods related to the team info files.
"""

import functools
import json
import os.path
import requests
import re

import pandas as pd
import sqlite3
import warnings

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.organization as organization

VARIANTS = {'WAS': 'WSH', 'L.A': 'LAK', 'T.B': 'TBL', 'S.J': 'SJS', 'N.J': 'NJD', 'CAL': 'CGY',
            'TB': 'TBL', 'LA': 'LAK', 'SJ': 'SJS', 'NJ': 'NJD', 'MON': 'MTL', 'LV': 'VGK', 'NAS': 'NSH'}


def get_team_info_filename():
    """
    Returns the team information filename

    :return: str, /scrape/data/other/TEAM_INFO.feather
    """
    return os.path.join(organization.get_other_data_folder(), 'teams.sqlite')


def get_team_info_file():
    """
    Returns the team information dataframe from memory. This is stored as a feather file for fast read/write.

    :return: dataframe from /scrape/data/other/TEAM_INFO.feather
    """
    return pd.read_sql_query('SELECT * FROM Teams', _TEAM_CONN)


def get_team_connection():
    """
    Get connection and cursor for schedule

    :return: connection
    """
    return sqlite3.connect(get_team_info_filename())


def get_team_info_url(teamid):
    """
    Gets the team url from the NHL API.

    :param teamid: int, the team ID

    :return: str, http://statsapi.web.nhl.com/api/v1/teams/[teamid]
    """
    return 'http://statsapi.web.nhl.com/api/v1/teams/{0:d}'.format(teamid)


def get_team_info_from_url(teamid):
    """
    Pulls ID, abbreviation, and name from the NHL API.

    :param teamid: int, the team ID

    :return: (id, abbrev, name)
    """

    teamid = int(teamid)
    page = helpers.try_url_n_times(get_team_info_url(teamid))
    if page is None:
        return None, None, None
    teaminfo = json.loads(page)

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


def _create_team_table():
    """
    Creates team table
    :return:
    """

    cols = ',\n'.join(['Team INT', 'Name CHAR', 'Abbreviation CHAR(3)'])
    query = 'CREATE TABLE Teams (\n{0:s},\nPRIMARY KEY ({1:s}))'.format(cols, 'Team')
    _TEAM_CURSOR.execute(query)
    _TEAM_CONN.commit()


def update_team_info(**kwargs):
    """
    Updates team info table. Does not Commit

    :param kwargs: keyword, value arguments for SQL row

    :return:
    """
    helpers._replace_into_table(_TEAM_CURSOR, 'Teams', **kwargs)


def write_team_ids_file(teamids=None):
    """
    Reads all team id URLs and stores information to disk. Has the following information:

    - ID: int
    - Abbreviation: str (three letters)
    - Name: str (full name)

    :param teamids: iterable of int. Tries to access team ids as listed in teamids. If not, goes from 1-110.

    :return: nothing
    """

    default_limit = 110
    if teamids is None:
        # Read from current team ids file, if it exists
        teamids = set(get_team_info_file().Team.values).union(set(range(1, default_limit + 1)))

    for i in teamids:
        try:
            tid, tabbrev, tname = get_team_info_from_url(i)
            if tid is None:
                continue
            update_team_info(Team=tid, Name=tname, Abbreviation=tabbrev)
        except requests.exceptions.HTTPError:
            pass
        except KeyError:
            pass
        except Exception as e:
            print(e, e.args)

    _TEAM_CONN.commit()


@functools.lru_cache(maxsize=10, typed=False)
def fix_variants(team):
    """
    E.g. changes WAS to WSH

    :param team: str
    :return: str
    """
    if team in VARIANTS:
        return VARIANTS[team]
    return team


@functools.lru_cache(maxsize=128, typed=False)
def team_as_id(team, silent=False):
    """
    A helper method. If team entered is int, returns that. If team is str, returns integer id of that team.

    :param team: int, or str
    :param silent: bool

    :return: int, the team ID
    """
    team = str(team)
    if re.match(r'^\d+\.?\d?$', team) is not None:
        return int(float(team))
    team = fix_variants(team)
    result = pd.read_sql_query('SELECT * FROM Teams WHERE Name = "{0:s}" OR Abbrevation = "{0:s}"'.format(
        team), _TEAM_CONN)
    if len(result) == 0:
        if not silent:
            warnings.warn('No results for ' + team)
        return None
    elif len(result) == 1:
        return int(result.Team.iloc[0])
    else:
        if not silent:
            warnings.warn('Multiple results for ' + team + '\nPlease use the long name')
        return None


@functools.lru_cache(maxsize=128, typed=False)
def team_as_str(team, abbreviation=True, silent=False):
    """
    A helper method. If team entered is str, returns that. If team is int, returns string name of that team.

    :param team: int, or str
    :param abbreviation: bool, whether to return 3-letter abbreviation or full name
    :param silent: bool

    :return: str, the team name
    """
    team = str(team)
    if re.match(r'^\d+\.?\d?$', team) is None:
        return team
    team = fix_variants(team)
    col_to_access = 'Abbreviation' if abbreviation else 'Name'
    result = pd.read_sql_query('SELECT * FROM Teams WHERE Team = {0:s}'.format(team), _TEAM_CONN)
    if len(result) == 0:
        if not silent:
            warnings.warn('No results for ' + team)
        return None
    elif len(result) == 1:
        return result[col_to_access].iloc[0]
    else:
        if not silent:
            warnings.warn('Multiple results for ' + team + '...???')
        return None


def get_team_colordict():
    """
    Get the team color dictionary

    :return: a dictionary of IDs to tuples of hex colors
    """
    return _TEAM_COLORS


def _get_team_colordict():
    """
    Run at startup to get the team color dictionary. Source: https://teamcolorcodes.com/category/nhl-team-color-codes/

    :return: a dictionary of names to tuples of hex colors
    """
    return {'ANA': ["#91764B", '#000000', '#EF5225'], 'ARI': ['#841F27', '#000000', '#EFE1C6'],
            'PHX': ['#841F27', '#000000', '#EFE1C6'], 'ATL': ['#4B82C3', '#CE7318'], 'BOS': ['#FFC422', '#000000'],
            'BUF': ['#002E62', '#FDBB2F', '#AEB6B9'], 'CGY': ['#E03A3E', '#FFC758', '#000000'],
            'CAR': ['#8E8E90', '#E03A3E', '#8E8E90'], 'CHI': ['#E3263A', '#000000'],
            'COL': ['#8B2942', '#01548A', '#000000', '#A9B0B8'],
            'CBJ': ['#00285C', '#E03A3E', '#A9B0B8'], 'DAL': ['#006A4E', '#000000', '#C0C0C0'],
            'DET': ['#EC1F26', '#FFFFFF'], 'EDM': ['#E66A20', '#003777'],
            'FLA': ['#C8213F', '#002E5F', '#D59C05'], 'LAK': ['#AFB7BA', '#000000'],
            'MIN': ['#025736', '#BF2B37', '#EFB410', '#EEE3C7'],
            'MTL': ['#213770', '#BF2F38'], 'NSH': ['#FDBB2F', '#002E62'], 'NJD': ['#E03A3E', '#000000'],
            'NYI': ['#F57D31', '#00529B'], 'NYR': ['#0161AB', '#E6393F'], 'OTT': ['#E4173E', '#000000', '#D69F0F'],
            'PHI': ['#F47940', '#000000'], 'PIT': ['#CCCC99', '#000000', '#FFCC33'],
            'SJS': ['#05535D', '#F38F20', '#000000'], 'STL': ['#0546A0', '#FFC325', '#101F48'],
            'TBL': ['#013E7D', '#000000', '#C0C0C0'], 'TOR': ['#003777', '#FFFFFF'],
            'VAN': ['#07346F', '#047A4A', '#A8A9AD'], 'LGK': ['#333F48', '#000000', '#89734C', '#C8102E;'],
            'WSH': ['#CF132B', '#00214E', '#000000'], 'WPG': ['#002E62', '#0168AB', '#A8A9AD']}


def get_team_colors(team):
    """
    Returns primary and secondary color for this team.

    :param team: str or int, the team

    :return: tuple of hex colors
    """
    return get_team_colordict()[team_as_str(team)]


def team_setup():
    """
    This method loads the team info df into memory

    :return: nothing
    """
    global _TEAM_COLORS
    try:
        _ = get_team_info_file()
    except pd.io.sql.DatabaseError:
        _create_team_table()
        write_team_ids_file()
    _TEAM_COLORS = _get_team_colordict()


_TEAM_CONN = get_team_connection()
_TEAM_CURSOR = _TEAM_CONN.cursor()
_TEAM_COLORS = None
team_setup()
