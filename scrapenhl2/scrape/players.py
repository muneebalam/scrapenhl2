"""
This module contains methods related to individual player info.
"""

import functools
import json
import os.path
import urllib.request
from tqdm import tqdm
import sqlite3

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
    cols = ',\n'.join(['PlayerID CHAR', 'DOB Date', 'Hand CHAR(1)', 'Weight INT',
                       'Height CHAR', 'Name CHAR', 'Nationality CHAR', 'Pos CHAR(1)'])
    query = 'CREATE TABLE Info (\n{0:s},\nPRIMARY KEY ({1:s}))'.format(cols, 'PlayerID')
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
    helpers._update_table(_PLAYER_CURSOR, 'Info', **kwargs)

def update_player_status(**kwargs):
    helpers._update_table(_PLAYER_CURSOR, 'Status', **kwargs)


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

    if not force_overwrite:
        # Pull only ones we don't have already
        newdf = pd.DataFrame({'PlayerID': [str(x) for x in playerids]})
        to_scrape = set(newdf.PlayerID).difference(get_player_info_file().PlayerID)
    else:
        to_scrape = playerids

    if len(to_scrape) == 0:
        return
    for playerid in tqdm(to_scrape, desc="Parsing players in play by play"):
        playerinfo = get_player_info_from_url(playerid)
        update_player_info(PlayerID=str(helpers.try_to_access_dict(playerinfo, 'ID')),
                           Name=helpers.try_to_access_dict(playerinfo, 'Name'),
                           Hand=helpers.try_to_access_dict(playerinfo, 'Hand', default_return=''),
                           Pos=helpers.try_to_access_dict(playerinfo, 'Pos', default_return=''),
                           DOB=helpers.try_to_access_dict(playerinfo, 'DOB'),
                           Weight=helpers.try_to_access_dict(playerinfo, 'Weight', default_return=0),
                           Height=helpers.try_to_access_dict(playerinfo, 'Height', default_return='') \
                                      .replace("'", '-').replace('"', ''),
                           Nationality=helpers.try_to_access_dict(playerinfo, 'Nationality', default_return=''))


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
def player_as_id(playername, filterids=None, dob=None):
    """
    A helper method. If player entered is int, returns that. If player is str, returns integer id of that player.

    :param playername: int, or str, the player whose names you want to retrieve
    :param filterids: a tuple of players to choose from. Needs to be tuple else caching won't work.
    :param dob: yyyy-mm-dd, use to help when multiple players have the same name

    :return: int, the player ID
    """
    filterdf = get_player_ids_file()
    if filterids is None:
        pass
    else:
        filterdf = filterdf.merge(pd.DataFrame({'ID': filterids}), how='inner', on='ID')
    pids = filterdf
    if dob is not None:
        pids = pids.query('DOB == "{0:s}"'.format(dob))
    if helpers.check_number(playername):
        return int(playername)
    elif isinstance(playername, str):
        df = pids.query('Name == "{0:s}"'.format(playername))
        if len(df) == 0:
            # ed.print_and_log('Could not find exact match for for {0:s}; trying exact substring match'.format(player))
            df = pids
            df = df[df.Name.str.contains(playername)]
            if len(df) == 0:
                # ed.print_and_log('Could not find exact substring match; trying fuzzy matching')
                name = helpers.fuzzy_match_player(playername, pids.Name)
                return player_as_id(name, tuple(filterdf.ID.values))
                # return player_as_id(name)
            elif len(df) == 1:
                return df.ID.iloc[0]
            else:
                print('Multiple results when searching for {0:s}; returning first result'.format(playername))
                print('You can specify a tuple of acceptable IDs to scrapenhl2.scrape.players.player_as_id')
                print(df.to_string())
                return df.ID.iloc[0]
        elif len(df) == 1:
            return df.ID.iloc[0]
        else:
            default = check_default_player_id(playername)
            if default is None:
                print('Multiple results when searching for {0:s}; returning first result'.format(playername))
                print('You can specify a tuple of acceptable IDs to scrapenhl2.scrape.players.player_as_id')
                print(df.to_string())
                return df.ID.iloc[0]
            else:
                print('Multiple results when searching for {0:s}; returning default'.format(playername))
                print('You can specify a tuple of acceptable IDs to scrapenhl2.scrape.players.player_as_id')
                print(df.to_string())
                return default
    else:
        print('Specified wrong type for player: {0:s}'.format(type(playername)))
        return None


def playerlst_as_str(players, filterdf=None):
    """
    Similar to player_as_str, but less robust against errors, and works on a list of players

    :param players: a list of int, or str, players whose names you want to retrieve
    :param filterdf: df, a dataframe of players to choose from. Defaults to all.

    :return: a list of str
    """
    if filterdf is None:
        filterdf = get_player_ids_file()
    df = pd.DataFrame({'ID': players})
    if df.ID.dtype == 'str' or df.ID.dtype == 'O':
        return df.ID
    else:
        df = df.merge(filterdf, how='left', on='ID')
        return df.Name


def playerlst_as_id(playerlst, exact=False, filterdf=None):
    """
    Similar to player_as_id, but less robust against errors, and works on a list of players.

    :param players: a list of int, or str, players whose IDs you want to retrieve.
    :param exact: bool. If True, looks for exact matches. If False, does not, using player_as_id (but will be slower)
    :param filterdf: df, a dataframe of players to choose from. Defaults to all.

    :return: a list of int/float
    """
    if filterdf is None:
        filterdf = get_player_ids_file()
    df = pd.DataFrame({'Name': playerlst})
    if not (df.Name.dtype == 'str' or df.Name.dtype == 'O'):
        return df.Name
    elif exact is True:
        return df.merge(filterdf, on='Name', how='left').PlayerID
    else:
        df.loc[:, 'ID'] = df.Name.apply(lambda x: player_as_id(x, tuple(filterdf.ID.values)))
        return df.ID


@functools.lru_cache(maxsize=128, typed=False)
def player_as_str(playerid, filterids=None):
    """
    A helper method. If player is int, returns string name of that player. Else returns standardized name.

    :param playerid: int, or str, player whose name you want to retrieve
    :param filterids: a tuple of players to choose from. Needs to be tuple else caching won't work.
        Probably not needed but you can use this method to go from part of the name to full name, in which case
        it may be helpful.

    :return: str, the player name
    """
    filterdf = get_player_ids_file()
    if filterids is None:
        pass
    else:
        filterdf = filterdf.merge(pd.DataFrame({'ID': filterids}), how='inner', on='ID')
    if isinstance(playerid, str):
        # full name
        newfilterdf = filterdf
        realid = player_as_id(playerid)
        return player_as_str(realid)
    elif helpers.check_number(playerid):
        player = int(playerid)
        df = filterdf.query('ID == {0:.0f}'.format(playerid))
        if len(df) == 0:
            print('Could not find name for {0:.0f}'.format(playerid))
            return None
        elif len(df) == 1:
            return df.Name.iloc[0]
        else:
            print('Multiple results when searching for {0:d}; returning first result'.format(playerid))
            print(df.to_string())
            return df.Name.iloc[0]
    else:
        print('Specified wrong type for player: {0:d}'.format(type(playerid)))
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


def update_player_ids_from_page(pbp):
    """
    Reads the list of players listed in the game file and adds to the player IDs file if they are not there already.

    :param pbp: json, the raw pbp

    :return: nothing
    """
    playerdict = pbp['gameData']['players']  # yields the subdictionary with players
    ids = [key[2:] for key in playerdict]  # keys are format "ID[PlayerID]"; pull that PlayerID part
    update_player_ids_file(ids)


def update_player_logs_from_page(pbp, season, game):
    """
    Takes the game play by play and adds players to the master player log file, noting that they were on the roster
    for this game, which team they played for, and their status (P for played, S for scratch).

    :param season: int, the season
    :param game: int, the game
    :param pbp: json, the pbp of the game

    :return: nothing
    """

    # Get players who played, and scratches, from boxscore
    home_played = helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'players')
    road_played = helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'players')
    home_scratches = str(helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'scratches'))
    road_scratches = str(helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'scratches'))

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
    for p in road_played:
        update_player_status(PlayerID=p, Season=season, Game=game, Team=gameinfo['Road'], Status='P')
    for p in road_scratches:
        update_player_status(PlayerID=p, Season=season, Game=game, Team=gameinfo['Road'], Status='S')

    # TODO: One issue is we do not see goalies (and maybe skaters) who dressed but did not play. How can this be fixed?

_PLAYER_CONN = get_player_info_connection()
_PLAYER_CURSOR = _PLAYER_CONN.cursor()
player_setup()
