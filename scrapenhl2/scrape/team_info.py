"""
This module contains methods related to the team info files.
"""

import functools
import json
import os.path
import urllib.error
import urllib.request

import feather
import pandas as pd

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.organization as organization


def get_team_info_filename():
    """
    Returns the team information filename

    :return: str, /scrape/data/other/TEAM_INFO.feather
    """
    return os.path.join(organization.get_other_data_folder(), 'TEAM_INFO.feather')


def _get_team_info_file():
    """
    Returns the team information dataframe from file. This is stored as a feather file for fast read/write.

    :return: dataframe from /scrape/data/other/TEAM_INFO.feather
    """
    return feather.read_dataframe(get_team_info_filename())


def get_team_info_file():
    """
    Returns the team information dataframe from memory. This is stored as a feather file for fast read/write.

    :return: dataframe from /scrape/data/other/TEAM_INFO.feather
    """
    return _TEAMS


def write_team_info_file(df):
    """
    Writes the team information file. This is stored as a feather file for fast read/write.

    :param df: the (team information) dataframe to write to file

    :returns: nothing
    """
    feather.write_dataframe(df, get_team_info_filename())
    team_setup()


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
        except Exception as e:
            print('Using default IDs')
            print(e, e.args)
            teamids = list(range(1, default_limit + 1))

    for i in teamids:
        try:
            tid, tabbrev, tname = get_team_info_from_url(i)
            if tid is None:
                continue
            ids.append(tid)
            abbrevs.append(tabbrev)
            names.append(tname)

            # ed.print_and_log('Done with ID # {0:d}: {1:s}'.format(tid, tname))
        except urllib.error.HTTPError:
            pass
        except Exception as e:
            print(e, e.args)

    teaminfo = pd.DataFrame({'ID': ids, 'Abbreviation': abbrevs, 'Name': names})
    write_team_info_file(teaminfo)


@functools.lru_cache(maxsize=128, typed=False)
def team_as_id(team):
    """
    A helper method. If team entered is int, returns that. If team is str, returns integer id of that team.

    :param team: int, or str

    :return: int, the team ID
    """
    if helpers.check_number(team):
        return int(team)
    elif isinstance(team, str):
        df = get_team_info_file().query('Name == "{0:s}" | Abbreviation == "{0:s}"'.format(team))
        if len(df) == 0:
            print('Could not find ID for {0:s}'.format(team))
            return None
        elif len(df) == 1:
            return df.ID.iloc[0]
        else:
            print('Multiple results when searching for {0:s}; returning first result'.format(team))
            print(df.to_string())
            return df.ID.iloc[0]
    else:
        print('Specified wrong type for team: {0:s}'.format(type(team)))
        return None


@functools.lru_cache(maxsize=128, typed=False)
def team_as_str(team, abbreviation=True):
    """
    A helper method. If team entered is str, returns that. If team is int, returns string name of that team.

    :param team: int, or str
    :param abbreviation: bool, whether to return 3-letter abbreviation or full name

    :return: str, the team name
    """
    col_to_access = 'Abbreviation' if abbreviation else 'Name'

    if isinstance(team, str):
        return team
    elif helpers.check_number(team):
        df = get_team_info_file().query('ID == {0:d}'.format(team))
        if len(df) == 0:
            try:
                result = add_team_to_info_file(team)
                if abbreviation:
                    return result[1]
                else:
                    return result[2]
            except Exception as e:
                print('Could not find name for {0:d} {1:s}'.format(team, str(e)))
                return None
        elif len(df) == 1:
            return df[col_to_access].iloc[0]
        else:
            print('Multiple results when searching for {0:d}; returning first result'.format(team))
            print(df.to_string())
            return df[col_to_access].iloc[0]
    else:
        print('Specified wrong type for team: {0:s}'.format(type(team)))
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
    global _TEAMS, _TEAM_COLORS
    if not os.path.exists(get_team_info_filename()):
        generate_team_ids_file()  # team IDs file
    _TEAMS = _get_team_info_file()
    _TEAM_COLORS = _get_team_colordict()


_TEAMS = None
_TEAM_COLORS = None
team_setup()
