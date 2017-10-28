"""
This module contains methods related to individual player info.
"""

import functools
import json
import os.path
import urllib.request

import feather
import pandas as pd

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.team_info as team_info


def get_player_log_file():
    """
    Returns the player log file from memory.
    :return: dataframe, the log
    """
    return _PLAYER_LOG


def _get_player_log_file():
    """
    Returns the player log file, reading from file. This is stored as a feather file for fast read/write.
    :return: dataframe from /scrape/data/other/PLAYER_LOG.feather
    """
    return feather.read_dataframe(get_player_log_filename())


def get_player_ids_file():
    """
    Returns the player information file. This is stored as a feather file for fast read/write.
    :return: /scrape/data/other/PLAYER_INFO.feather
    """
    return _PLAYERS


def _get_player_ids_file():
    """
    Runs at startup to read the player information file. This is stored as a feather file for fast read/write.
    :return: /scrape/data/other/PLAYER_INFO.feather
    """
    return feather.read_dataframe(get_player_ids_filename())


def write_player_log_file(df):
    """
    Writes the given dataframe to file as the player log filename
    :param df: pandas dataframe
    :return: nothing
    """
    feather.write_dataframe(df.drop_duplicates(), get_player_log_filename())
    player_setup()


def get_player_log_filename():
    """
    Returns the player log filename.
    :return: str, /scrape/data/other/PLAYER_LOG.feather
    """
    return os.path.join(organization.get_other_data_folder(), 'PLAYER_LOG.feather')


def get_player_ids_filename():
    return os.path.join(organization.get_other_data_folder(), 'PLAYER_INFO.feather')


def check_default_player_id(playername):
    """
    E.g. For Mike Green, I should automatically assume we mean 8471242 (WSH/DET), not 8468436.
    Returns None if not in dict.
    :param playername: str
    :return: int, or None
    """
    # TODO gradually add to this
    return helpers.try_to_access_dict({'Mike Green': 8471242}, playername)


def player_setup():
    """
    Loads team info file into memory.
    :return: nothing
    """
    global _PLAYERS, _PLAYER_LOG

    _PLAYERS = _get_player_ids_file()
    _PLAYER_LOG = _get_player_log_file()


def rescrape_player(playerid):
    """
    If you notice that a player name, position, etc, is outdated, call this method on their ID. It will
    re-scrape their data from the NHL API.
    :param playerid: int, their ID. Also accepts str, their name.
    :return: nothing
    """
    playerid = player_as_id(playerid)
    update_player_ids_file(playerid, True)


def write_player_ids_file(df):
    """
    Writes the given dataframe to disk as the player ids mapping.
    :param df: pandas dataframe, player ids file
    :return: nothing
    """
    feather.write_dataframe(df, get_player_ids_filename())


def get_player_url(playerid):
    """
    Gets the url for a page containing information for specified player from NHL API.
    :param playerid: int
        the player ID
    :return: str
        https://statsapi.web.nhl.com/api/v1/people/[playerid]
    """
    return 'https://statsapi.web.nhl.com/api/v1/people/{0:s}'.format(str(playerid))


def update_player_ids_file(playerids, force_overwrite=False):
    """
    Adds these entries to player IDs file if need be.
    :param playerids: a list of IDs
    :param force_overwrite: bool. If True, will re-scrape data for all player ids. If False, only new ones.
    :return: nothing
    """
    # In case we get just one number
    if isinstance(playerids, int):
        playerids = [playerids]

    ids = []
    names = []
    hands = []
    pos = []
    dobs = []
    heights = []
    weights = []
    nationalities = []

    current_players = get_player_ids_file()

    if not force_overwrite:
        # Pull only ones we don't have already
        newdf = pd.DataFrame({'ID': [int(x) for x in playerids]})
        to_scrape = set(newdf.ID).difference(current_players.ID)
    else:
        to_scrape = playerids
        current_players = current_players.merge(pd.DataFrame({'ID': playerids}),
                                                how='outer',
                                                on='ID')
        current_players = current_players.query('_merge == "left_only"').drop('_merge', axis=1)
    if len(to_scrape) == 0:
        return
    for playerid in to_scrape:
        playerinfo = get_player_info_from_url(playerid)
        ids.append(playerinfo['ID'])
        names.append(playerinfo['Name'])
        hands.append(playerinfo['Hand'])
        pos.append(playerinfo['Pos'])
        dobs.append(playerinfo['DOB'])
        weights.append(playerinfo['Weight'])
        heights.append(playerinfo['Height'])
        nationalities.append(playerinfo['Nationality'])
    df = pd.DataFrame({'ID': ids, 'Name': names, 'DOB': dobs, 'Hand': hands, 'Pos': pos,
                       'Weight': weights, 'Height': heights, 'Nationality': nationalities})
    df.loc[:, 'ID'] = pd.to_numeric(df.ID).astype(int)
    write_player_ids_file(pd.concat([df, current_players]))
    # print(len(_PLAYERS.groupby('ID').count().query('Name >= 2'))) # not getting duplicates, so I think we're okay


def update_player_log_file(playerids, seasons, games, teams, statuses):
    """
    Updates the player log file with given players. The player log file notes which players played in which games
    and whether they were scratched or played.
    :param playerids: int or str or list of int
    :param seasons: int, the season, or list of int the same length as playerids
    :param games: int, the game, or list of int the same length as playerids
    :param teams: str or int, the team, or list of int the same length as playerids
    :param statuses: str, or list of str the same length as playerids
    :return: nothing
    """

    # Change everything to lists first if need be
    if isinstance(playerids, int) or isinstance(playerids, str):
        playerids = player_as_id(playerids)
        playerids = [playerids]
    if helpers.check_number(seasons):
        seasons = [seasons for _ in range(len(playerids))]
    if helpers.check_number(games):
        games = [games for _ in range(len(playerids))]
    if helpers.check_types(teams):
        teams = team_info.team_as_id(teams)
        teams = [teams for _ in range(len(playerids))]
    if isinstance(statuses, str):
        statuses = [statuses for _ in range(len(playerids))]

    df = pd.DataFrame({'ID': playerids,  # Player ID
                       'Team': teams,  # Team
                       'Status': statuses,  # P for played, S for scratch.
                       'Season': seasons,  # Season
                       'Game': games})  # Game
    if len(get_player_log_file()) == 1:
        # In this case, the only entry is our original entry for Ovi, that sets the datatypes properly
        write_player_log_file(df)
    else:
        write_player_log_file(pd.concat([get_player_log_file(), df]))


@functools.lru_cache(maxsize=128, typed=False)
def player_as_id(player):
    """
    A helper method. If player entered is int, returns that. If player is str, returns integer id of that player.
    :param player: int, or str
    :return: int, the player ID
    """
    pids = get_player_ids_file()
    if helpers.check_number(player):
        return int(player)
    elif isinstance(player, str):
        df = pids.query('Name == "{0:s}"'.format(player))
        if len(df) == 0:
            # ed.print_and_log('Could not find exact match for for {0:s}; trying exact substring match'.format(player))
            df = pids
            df = df[df.Name.str.contains(player)]
            if len(df) == 0:
                # ed.print_and_log('Could not find exact substring match; trying fuzzy matching')
                return player_as_id(helpers.fuzzy_match_player(player, pids.Name))
            elif len(df) == 1:
                return df.ID.iloc[0]
            else:
                print('Multiple results when searching for {0:s}; returning first result'.format(player))
                print(df.to_string())
                return df.ID.iloc[0]
        elif len(df) == 1:
            return df.ID.iloc[0]
        else:
            default = check_default_player_id(player)
            if default is None:
                print('Multiple results when searching for {0:s}; returning first result'.format(player))
                print(df.to_string())
                return df.ID.iloc[0]
            else:
                print('Multiple results when searching for {0:s}; returning default'.format(player))
                print(df.to_string())
                return default
    else:
        print('Specified wrong type for player: {0:s}'.format(type(player)))
        return None


def playerlst_as_str(players):
    """
    Similar to player_as_str, but less robust against errors, and works on a list of players
    :param players: a list of int, or str
    :return: a list of str
    """
    df = pd.DataFrame({'ID': players})
    if df.ID.dtype == 'str':
        return df.ID
    else:
        df = df.merge(get_player_ids_file(), how='left', on='ID')
        return df.Name


def playerlst_as_id(players, exact=False):
    """
    Similar to player_as_id, but less robust against errors, and works on a list of players.
    :param players: a list of int, or str
    :param exact: bool. If True, looks for exact matches. If False, does not, using player_as_id (but will be slower)
    :return: a list of int/float
    """
    df = pd.DataFrame({'Name': players})
    if df.Name.dtype != 'str':
        return df.Name
    elif exact is False:
        return df.merge(get_player_ids_file(), on='Name', how='left').ID
    else:
        df.loc[:, 'ID'] = df.Name.apply(lambda x: player_as_id(x))
        return df.ID


@functools.lru_cache(maxsize=128, typed=False)
def player_as_str(player):
    """
    A helper method. If player is int, returns string name of that player. Else returns standardized name.
    :param player: int, or str
    :return: str, the player name
    """
    if isinstance(player, str):
        return player_as_str(player_as_id(player))
    elif helpers.check_number(player):
        player = int(player)
        df = get_player_ids_file().query('ID == {0:d}'.format(player))
        if len(df) == 0:
            print('Could not find name for {0:d}'.format(player))
            return None
        elif len(df) == 1:
            return df.Name.iloc[0]
        else:
            print('Multiple results when searching for {0:d}; returning first result'.format(player))
            print(df.to_string())
            return df.Name.iloc[0]
    else:
        print('Specified wrong type for player: {0:d}'.format(type(player)))
        return None


def get_player_info_from_url(playerid):
    """
    Gets ID, Name, Hand, Pos, DOB, Height, Weight, and Nationality from the NHL API.
    :param playerid: int, the player id
    :return: dict with player ID, name, handedness, position, etc
    """
    with urllib.request.urlopen(get_player_url(playerid)) as reader:
        page = reader.read().decode('latin-1')
    data = json.loads(page)

    info = {}
    vars_to_get = {'ID': ['people', 0, 'id'],
                   'Name': ['people', 0, 'fullName'],
                   'Hand': ['people', 0, 'shootsCatches'],
                   'Pos': ['people', 0, 'primaryPosition', 'code'],
                   'DOB': ['people', 0, 'birthDate'],
                   'Height': ['people', 0, 'height'],
                   'Weight': ['people', 0, 'weight'],
                   'Nationality': ['people', 0, 'nationality']}
    for key, val in vars_to_get.items():
        info[key] = helpers.try_to_access_dict(data, *val)

    # Remove the space in the middle of height
    if info['Height'] is not None:
        info['Height'] = info['Height'].replace(' ', '')
    return info


def generate_player_ids_file():
    """
    Creates a dataframe with these columns:

    - ID: int, player ID
    - Name: str, player name
    - DOB: str, date of birth
    - Hand: char, R or L
    - Pos: char, one of C/R/L/D/G

    It will be populated with Alex Ovechkin to start.
    :return: nothing
    """
    df = pd.DataFrame({'ID': [8471214],
                       'Name': ['Alex Ovechkin'],
                       'DOB': ['1985-09-17'],
                       'Hand': ['R'],
                       'Pos': ['L'],
                       'Height': ["6'3\""],
                       'Weight': [235],
                       'Nationality': ['RUS']})
    write_player_ids_file(df)
    player_setup()


def generate_player_log_file():
    """
    Run this when no player log file exists already. This is for getting the datatypes right. Adds Alex Ovechkin
    in Game 1 vs Pittsburgh in 2016-2017.
    :return: nothing
    """
    df = pd.DataFrame({'ID': [8471214],  # Player ID (Ovi)
                       'Team': [15],  # Team (WSH)
                       'Status': ['P'],  # P for played, S for scratch.  # TODO can I do healthy vs injured?
                       'Season': [2016],  # Season (2016-17)
                       'Game': [30221]})  # Game (G1 vs PIT)
    if os.path.exists(get_player_log_filename()):
        pass  # ed.print_and_log('Warning: overwriting existing player log with default, one-line df!', 'warn')
    write_player_log_file(df)




_PLAYERS = None
_PLAYER_LOG = None
player_setup()