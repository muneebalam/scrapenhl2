"""
This module contains methods related to individual player info.
"""

import functools
import json
import os.path
import urllib.request
from tqdm import tqdm
import sqlite3
import re
import warnings

import feather
import pandas as pd

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.team_info as team_info


def get_player_info_filename():
    """
    Returns other data / playerlog.sqlite
    :return:
    """
    return os.path.join(organization.get_other_data_folder(), "playerlog.sqlite")


def get_player_info_connection():
    """
    Returns connection to player info file
    :return:
    """
    return sqlite3.connect(get_player_info_filename())


def _create_player_info_table():
    """Creates Info DOB, Hand, Height, PlayerID, Name, Nationality, Pos, Weight"""
    cols = ',\n'.join(['PlayerID CHAR', 'Team INT', 'DOB Date', 'Hand CHAR(1)', 'Weight INT',
                       'Height CHAR', 'Name CHAR', 'Nationality CHAR', 'Pos CHAR(1)'])
    query = 'CREATE TABLE Info (\n{0:s},\nPRIMARY KEY ({1:s}, {2:s}))'.format(cols, 'PlayerID', 'Team')
    _PLAYER_CURSOR.execute(query)
    _PLAYER_CONN.commit()


def _create_player_status_table():
    """Creates Status, Season, Game, PlayerID, Team, Status"""
    cols = ',\n'.join(['Season INT', 'Game INT', 'PlayerID CHAR', 'Team INT', 'Status CHAR(1)'])
    query = 'CREATE TABLE Status (\n{0:s},\nPRIMARY KEY ({1:s}, {2:s}, {3:s}, {4:s}))'.format(cols, 'Season', 'Game',
                                                                                       'PlayerID', 'Team')
    _PLAYER_CURSOR.execute(query)
    _PLAYER_CONN.commit()


def get_player_info_file():
    """
    Returns the player information file (SELECT * FROM info)

    :return: df
    """
    return pd.read_sql_query('SELECT * FROM Info', _PLAYER_CONN)


def get_player_status_file():
    """
    Returns player status file

    :return:
    """
    return pd.read_sql_query('SELECT * FROM Status', _PLAYER_CONN)


def check_default_player_id(playername):
    """
    E.g. For Mike Green, I should automatically assume we mean 8471242 (WSH/DET), not 8468436.
    Returns None if not in dict.
    Ideally improve code so this isn't needed.

    :param playername: str

    :return: int, or None
    """
    # TODO gradually add to this
    return helpers.try_to_access_dict({'Mike Green': 8471242,
                                       'Francois Beauchemin': 8467400,
                                       'Erik Karlsson': 8474578,
                                       'Mike Hoffman': 8474884,
                                       'Tyler Johnson': 8474870,
                                       'Josh Anderson': 8476981,
                                       'Sebastian Aho': 8478427,
                                       'Trevor Lewis': 8473453,
                                       'Ryan Murphy': 8476465}, playername)


def player_setup():
    """
    Loads team info file into memory.

    :return: nothing
    """
    try:
        _ = get_player_info_file()
    except pd.io.sql.DatabaseError:
        _create_player_info_table()

    try:
        _ = get_player_status_file()
    except pd.io.sql.DatabaseError:
        _create_player_status_table()


def get_player_url(playerid):
    """
    Gets the url for a page containing information for specified player from NHL API.

    :param playerid: int, the player ID

    :return: str, https://statsapi.web.nhl.com/api/v1/people/[playerid]
    """
    return 'https://statsapi.web.nhl.com/api/v1/people/{0:s}'.format(str(playerid))


def update_player_info(**kwargs):
    helpers._replace_into_table(_PLAYER_CURSOR, 'Info', **kwargs)

def update_player_status(**kwargs):
    helpers._replace_into_table(_PLAYER_CURSOR, 'Status', **kwargs)


def update_player_ids_file(playerids, team, force_overwrite=False):
    """
    Adds these entries to player IDs file if need be.

    :param playerids: a list of IDs
    :param team: int, just one at a time
    :param force_overwrite: bool. If True, will re-scrape data for all player ids. If False, only new ones.

    :return: nothing
    """
    # In case we get just one number
    if isinstance(playerids, int):
        playerids = [playerids]

    if not force_overwrite:
        # Pull only ones we don't have already
        newdf = pd.DataFrame({'PlayerID': [str(x) for x in playerids]})
        to_scrape = set(newdf.PlayerID).difference(get_player_info_file().PlayerID)
    else:
        to_scrape = playerids

    if len(to_scrape) == 0:
        return
    #for playerid in tqdm(to_scrape, desc="Parsing players in play by play"):
    for playerid in playerids:
        if player_as_str(playerid, silent=True) is not None and not force_overwrite:
            continue
        playerinfo = get_player_info_from_url(playerid)
        update_player_info(PlayerID=str(helpers.try_to_access_dict(playerinfo, 'ID')),
                           Team=team,
                           Name=helpers.try_to_access_dict(playerinfo, 'Name'),
                           Hand=helpers.try_to_access_dict(playerinfo, 'Hand', default_return=''),
                           Pos=helpers.try_to_access_dict(playerinfo, 'Pos', default_return=''),
                           DOB=helpers.try_to_access_dict(playerinfo, 'DOB'),
                           Weight=helpers.try_to_access_dict(playerinfo, 'Weight', default_return=0),
                           Height=helpers.try_to_access_dict(playerinfo, 'Height', default_return='') \
                                      .replace("'", '-').replace('"', ''),
                           Nationality=helpers.try_to_access_dict(playerinfo, 'Nationality', default_return=''))
    _PLAYER_CONN.commit()


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

    for season, game, team, playerid, status in zip(seasons, games, teams, playerids, statuses):
        update_player_status(Season=season, Game=game, Team=team, PlayerID=playerid, Status=status)


@functools.lru_cache(maxsize=128, typed=False)
def get_player_position(player):
    """
    Retrieves position of player

    :param player: str or int, the player name or ID

    :return: str, player position (e.g. C, D, R, L, G)
    """

    df = get_player_ids_file()
    df = df[df.ID == player_as_id(player)]
    if len(df) == 1:
        return df.Pos.iloc[0]
    else:
        print('Could not find position for', player)
        return None


@functools.lru_cache(maxsize=128, typed=False)
def get_player_handedness(player):
    """
    Retrieves handedness of player

    :param player: str or int, the player name or ID

    :return: str, player hand (L or R)
    """

    df = get_player_ids_file()
    df = df[df.ID == player_as_id(player)]
    if len(df) == 1:
        return df.Hand.iloc[0]
    else:
        print('Could not find hand for', player)
        return None


@functools.lru_cache(maxsize=128, typed=False)
def player_as_id(playername, team=None, dob=None, silent=False):
    """
    A helper method. If player entered is int, returns that. If player is str, returns integer id of that player.

    :param playername: int, or str, the player whose names you want to retrieve
    :param team: int
    :param dob: yyyy-mm-dd, use to help when multiple players have the same name
    :param silent: bool

    :return: int, the player ID
    """

    # If playerid is an int or float, return it
    if re.match(r'^\d+\.?\d?$', playername) is not None:
        return playername

    query = 'SELECT * FROM Info WHERE Name LIKE "%{0:s}"'.format(playername)  # matches full or last name
    if team is not None:
        query += ' AND Team = {0:d}'.format(team_info.team_as_id(team))
    if dob is not None:
        query += ' AND DOB = "{0:s}"'.format(dob)
    result = pd.read_sql_query(query, _PLAYER_CONN)

    if len(result) == 0:
        # Fuzzy match
        result = helpers.fuzzy_match_player(playername, get_player_info_file())
        if len(result) == 0:
            if not silent:
                warnings.warn('No results for ' + playername)
            return None
        elif len(result) == 1:
            return result.Name.iloc[0]
        else:
            if not silent:
                warnings.warn('Multiple results for ' + playername + '\nPlease specify a team')
            return None
    if len(result) == 1:
        return result.Name.iloc[0]
    else:
        if not silent:
            warnings.warn('Multiple results for ' + playername + '\nPlease specify a team')
        return None


def playerlst_as_str(players, filterdf=None):
    """
    Similar to player_as_str, but less robust against errors, and works on a list of players

    :param players: a list of int, or str, players whose names you want to retrieve
    :param filterdf: df, a dataframe of players to choose from. Defaults to all.

    :return: a list of str
    """
    raise TypeError


def playerlst_as_id(playerlst, exact=False, filterdf=None):
    """
    Similar to player_as_id, but less robust against errors, and works on a list of players.

    :param players: a list of int, or str, players whose IDs you want to retrieve.
    :param exact: bool. If True, looks for exact matches. If False, does not, using player_as_id (but will be slower)
    :param filterdf: df, a dataframe of players to choose from. Defaults to all.

    :return: a list of int/float
    """
    raise TypeError


@functools.lru_cache(maxsize=128, typed=False)
def player_as_str(playerid, team=None, silent=False):
    """
    A helper method. If player is int, returns string name of that player. Else returns standardized name.

    :param playerid: int, or str, player whose name you want to retrieve
    :param team: a team. In case there are multiple matches (e.g. Sebastian Aho), can separate them.
    :param silent: bool

    :return: str, the player name
    """

    # If playerid is not an int or float, return it
    if re.match(r'^\d+\.?\d?$', playerid) is None:
        return playerid

    query = 'SELECT * FROM Info WHERE PlayerID = "{0:s}"'.format(playerid)
    result = pd.read_sql_query(query, _PLAYER_CONN)

    if len(result) == 0:
        if not silent:
            warnings.warn('No results for ' + playerid)
        return None
    if len(result) == 1:
        return result.Name.iloc[0]
    else:
        if not silent:
            warnings.warn('Multiple results for ' + playerid + '\nPlease specify a team')
        return None


def get_player_info_from_url(playerid):
    """
    Gets ID, Name, Hand, Pos, DOB, Height, Weight, and Nationality from the NHL API.

    :param playerid: int, the player id

    :return: dict with player ID, name, handedness, position, etc
    """
    page = helpers.try_url_n_times(get_player_url(playerid))
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


def update_player_logs_from_page(pbp, season, game):
    """
    Takes the game play by play and adds players to the master player log file, noting that they were on the roster
    for this game, which team they played for, and their status (P for played, S for scratch).

    Also updates player IDS file.

    :param season: int, the season
    :param game: int, the game
    :param pbp: json, the pbp of the game

    :return: nothing
    """

    # Get players who played, and scratches, from boxscore
    home_played = helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'players')
    road_played = helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'players')
    home_scratches = [str(x) for x in helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home',
                                                                 'scratches')]
    road_scratches = [str(x) for x in helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away',
                                                                 'scratches')]

    # Played are both dicts, so make them lists
    home_played = [pid[2:] for pid in home_played]
    road_played = [pid[2:] for pid in road_played]

    # Played may include scratches, so make sure to remove them
    home_played = list(set(home_played).difference(set(home_scratches)))
    road_played = list(set(road_played).difference(set(road_scratches)))

    # Get home and road names
    gameinfo = schedules.get_game_data_from_schedule(season, game)

    # Update player logs
    for p in home_played:
        update_player_status(PlayerID=p, Season=season, Game=game, Team=gameinfo['Home'], Status='P')
    for p in home_scratches:
        update_player_status(PlayerID=p, Season=season, Game=game, Team=gameinfo['Home'], Status='S')
    update_player_ids_file([*home_played, *home_scratches], team=gameinfo['Home'])

    for p in road_played:
        update_player_status(PlayerID=p, Season=season, Game=game, Team=gameinfo['Road'], Status='P')
    for p in road_scratches:
        update_player_status(PlayerID=p, Season=season, Game=game, Team=gameinfo['Road'], Status='S')
    update_player_ids_file([*road_played, *road_scratches], team=gameinfo['Road'])

    # TODO: One issue is we do not see goalies (and maybe skaters) who dressed but did not play. How can this be fixed?

_PLAYER_CONN = get_player_info_connection()
_PLAYER_CURSOR = _PLAYER_CONN.cursor()
player_setup()
