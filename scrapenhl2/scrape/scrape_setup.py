"""
This module contains several helpful methods for accessing files.
At import, this module creates folders for data storage if need be.

It also creates a team ID mapping and schedule files from 2005 through the current season (if the files do not exist).
"""

import os
import os.path
import feather
import pandas as pd
import json
import os.path
import urllib.request
import urllib.error
import datetime
import numpy as np
import logging


def _print_and_log(message, level='info'):
    """
    A helper method that prints message to console and also writes to log with specified level
    :param message: str, the message
    :param level: str, the level of log: info, warn, error, critical
    :return: nothing
    """
    print(message)
    if level == 'warn':
        logging.warning(message)
    elif level == 'error':
        logging.error(message)
    elif level == 'critical':
        logging.critical(message)
    else:
        logging.info(message)


def _get_current_season():
    """
    Runs at import only. Sets current season as today's year minus 1, or today's year if it's September or later
    :return: int, current season
    """
    season = datetime.datetime.now().year - 1
    if datetime.datetime.now().month >= 9:
        season += 1
    return season


def get_current_season():
    """
    Returns the current season.
    :return: The current season variable (generated at import from _get_current_season)
    """
    return _CURRENT_SEASON


def _get_base_dir():
    """
    Returns the base directory (two directories up from __file__)
    :return: the base directory
    """
    return '../../'  # Formerly (os.path.join(*(__file__.split('/')[:-2])))


def get_base_dir():
    """
    Returns the base directory of this package.
    :return: the base directory (generated at import from _get_base_dir)
    """
    return _BASE_DIR


def _check_create_folder(*args):
    """
    A helper method to create a folder if it doesn't exist already
    :param args: the parts of the filepath. These are joined together with the base directory
    :return: nothing
    """
    path = os.path.join(get_base_dir(), *args)
    if not os.path.exists(path):
        os.makedirs(path)


def _create_folders_and_files():
    """
    Creates folders for data storage if needed:

    - /scrape/data/raw/pbp/[seasons]/
    - /scrape/data/raw/toi/[seasons]/
    - /scrape/data/parsed/pbp/[seasons]/
    - /scrape/data/parsed/toi/[seasons]/
    - /scrape/data/teams/pbp/[seasons]/
    - /scrape/data/teams/toi/[seasons]/
    - /scrape/data/other/

    Also creates team IDs file and season schedule files if they don't exist already.
    :return: nothing
    """
    # ------- Raw -------
    for season in range(2005, _CURRENT_SEASON + 1):
        _check_create_folder(get_season_raw_pbp_folder(season))
    for season in range(2005, _CURRENT_SEASON + 1):
        _check_create_folder(get_season_raw_toi_folder(season))

    # ------- Parsed -------
    for season in range(2005, _CURRENT_SEASON + 1):
        _check_create_folder(get_season_parsed_pbp_folder(season))
    for season in range(2005, _CURRENT_SEASON + 1):
        _check_create_folder(get_season_parsed_toi_folder(season))

    # ------- Team logs -------
    for season in range(2005, _CURRENT_SEASON + 1):
        _check_create_folder(get_season_team_pbp_folder(season))
    for season in range(2005, _CURRENT_SEASON + 1):
        _check_create_folder(get_season_team_toi_folder(season))

    # ------- Other stuff -------
    _check_create_folder(get_other_data_folder())

    if not os.path.exists(get_team_info_filename()):
        generate_team_ids_file()  # team IDs file

    for season in range(2005, _CURRENT_SEASON + 1):
        if not os.path.exists(get_season_schedule_filename(season)):
            generate_season_schedule_file(season)  # season schedule
        # There is a potential issue here for current season.
        # For current season, we'll update this as we go along.
        # But original creation first time you start up in a new season is automatic, here.
        # When we autoupdate season date, we need to make sure to re-access this file and add in new entries

    if not os.path.exists(get_player_ids_filename()):
        generate_player_ids_file()

    if not os.path.exists(get_player_log_filename()):
        generate_player_log_file()


def get_game_raw_pbp_filename(season, game):
    """
    Returns the filename of the raw pbp folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/raw/pbp/[season]/[game].zlib
    """
    return os.path.join(get_season_raw_pbp_folder(season), str(game) + '.zlib')


def get_game_raw_toi_filename(season, game):
    """
    Returns the filename of the raw toi folder
    :param season: int, current season
    :param game: int, game
    :return:  /scrape/data/raw/toi/[season]/[game].zlib
    """
    return os.path.join(get_season_raw_toi_folder(season), str(game) + '.zlib')


def get_game_parsed_pbp_filename(season, game):
    """
    Returns the filename of the parsed pbp folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/parsed/pbp/[season]/[game].zlib
    """
    return os.path.join(get_season_parsed_pbp_folder(season), str(game) + '.h5')


def get_game_parsed_toi_filename(season, game):
    """
    Returns the filename of the parsed toi folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/parsed/toi/[season]/[game].zlib
    """
    return os.path.join(get_season_parsed_toi_folder(season), str(game) + '.h5')


def get_raw_data_folder():
    """
    Returns the folder containing raw data
    :return: /scrape/data/raw/
    """
    return os.path.join('data', 'raw')


def get_parsed_data_folder():
    """
    Returns the folder containing parsed data
    :return: /scrape/data/parsed/
    """
    return os.path.join('data', 'parsed')


def get_team_data_folder():
    """
    Returns the folder containing team log data
    :return: /scrape/data/teams/
    """
    return os.path.join('data', 'teams')


def get_other_data_folder():
    """
    Returns the folder containing other data
    :return: /scrape/data/other/
    """
    return os.path.join('data', 'other')


def get_season_raw_pbp_folder(season):
    """
    Returns the folder containing raw pbp for given season
    :param season: int, current season
    :return: /scrape/data/raw/pbp/[season]/
    """
    return os.path.join(get_raw_data_folder(), 'pbp', str(season))


def get_season_raw_toi_folder(season):
    """
    Returns the folder containing raw toi for given season
    :param season: int, current season
    :return: /scrape/data/raw/toi/[season]/
    """
    return os.path.join(get_raw_data_folder(), 'toi', str(season))


def get_season_parsed_pbp_folder(season):
    """
    Returns the folder containing parsed pbp for given season
    :param season: int, current season
    :return: /scrape/data/parsed/pbp/[season]/
    """
    return os.path.join(get_parsed_data_folder(), 'pbp', str(season))


def get_season_parsed_toi_folder(season):
    """
    Returns the folder containing parsed toi for given season
    :param season: int, current season
    :return: /scrape/data/raw/toi/[season]/
    """
    return os.path.join(get_parsed_data_folder(), 'toi', str(season))


def get_season_team_pbp_folder(season):
    """
    Returns the folder containing team pbp logs for given season
    :param season: int, current season
    :return: /scrape/data/teams/pbp/[season]/
    """
    return os.path.join(get_team_data_folder(), 'pbp', str(season))


def get_season_team_toi_folder(season):
    """
    Returns the folder containing team toi logs for given season
    :param season: int, current season
    :return: /scrape/data/teams/toi/[season]/
    """
    return os.path.join(get_team_data_folder(), 'toi', str(season))


def get_team_pbp_filename(season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    return os.path.join(get_season_team_pbp_folder(season),
                        "{0:s}.feather".format(team_as_str(team, abbreviation=True)))


def get_team_toi_filename(season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    return os.path.join(get_season_team_toi_folder(season),
                        "{0:s}.feather".format(team_as_str(team, abbreviation=True)))


def get_team_pbp(season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    return feather.read_dataframe(get_team_pbp_filename(season, team_as_str(team, True)))


def get_team_toi(season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    return feather.read_dataframe(get_team_toi_filename(season, team_as_str(team, True)))


def write_team_pbp(pbp, season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    if pbp is None:
        _print_and_log('PBP df is None, will not write team log', 'warn')
        return
    feather.write_dataframe(pbp, get_team_pbp_filename(season, team_as_str(team, True)))


def write_team_toi(toi, season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    if toi is None:
        _print_and_log('TOI df is None, will not write team log', 'warn')
        return
    try:
        feather.write_dataframe(toi, get_team_toi_filename(season, team_as_str(team, True)))
    except ValueError:
        # Need dtypes to be numbers or strings. Sometimes get objs instead
        for col in toi:
            try:
                toi.loc[:, col] = pd.to_numeric(toi[col])
            except ValueError:
                toi.loc[:, col] = toi[col].astype(str)
        feather.write_dataframe(toi, get_team_toi_filename(season, team_as_str(team, True)))



def get_team_info_filename():
    """
    Returns the team information filename
    :return: /scrape/data/other/TEAM_INFO.feather
    """
    return os.path.join(get_other_data_folder(), 'TEAM_INFO.feather')


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


def get_team_info_url(teamid):
    """
    Gets the team url from the NHL API.
    :param teamid: int
    :return: http://statsapi.web.nhl.com/api/v1/teams/[teamid]
    """
    return 'http://statsapi.web.nhl.com/api/v1/teams/{0:d}'.format(teamid)


def generate_team_ids_file(limit=110):
    """
    Reads all team id URLs and stores information to disk. Has the following information:

    - ID: int
    - Abbreviation: str (three letters)
    - Name: str (full name)

    :param limit: int. Tries every team ID from 1 to limit (inclusive)
    :return: nothing
    """
    _print_and_log('Creating team IDs file')
    ids = []
    abbrevs = []
    names = []

    for i in range(1, limit + 1):
        url = get_team_info_url(i)
        try:
            with urllib.request.urlopen(url) as reader:
                page = reader.read()
            teaminfo = json.loads(page.decode('latin-1'))

            tid = teaminfo['teams'][0]['id']
            tabbrev = teaminfo['teams'][0]['abbreviation']
            tname = teaminfo['teams'][0]['name']

            ids.append(tid)
            abbrevs.append(tabbrev)
            names.append(tname)

            _print_and_log('Done with ID # {0:d}: {1:s}'.format(tid, tname))

        except urllib.error.HTTPError:
            pass

    teaminfo = pd.DataFrame({'ID': ids, 'Abbreviation': abbrevs, 'Name': names})
    write_team_info_file(teaminfo)
    _print_and_log('Done writing team IDs')


def get_season_schedule_url(season):
    """
    Gets the url for a page containing all of this season's games (Sep 1 to Jun 26) from NHL API.
    :param season: int, the season
    :return: https://statsapi.web.nhl.com/api/v1/schedule?startDate=[season]-09-01&endDate=[season+1]-06-25
    """
    return 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=' \
           '{0:d}-09-01&endDate={1:d}-06-25'.format(season, season + 1)


def get_game_url(season, game):
    """
    Gets the url for a page containing information for specified game from NHL API.
    :param season: int, the season
    :param game: int, the game
    :return: https://statsapi.web.nhl.com/api/v1/game/[season]0[game]/feed/live
    """
    return 'https://statsapi.web.nhl.com/api/v1/game/{0:d}0{1:d}/feed/live'.format(season, game)


def get_shift_url(season, game):
    """
    Gets the url for a page containing shift information for specified game from NHL API.
    :param season: int, the season
    :param game: int, the game
    :return : str, http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId=[season]0[game]
    """
    return 'http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId={0:d}0{1:d}'.format(season, game)


def get_player_url(playerid):
    """
    Gets the url for a page containing information for specified player from NHL API.
    :param playerid: int, the player ID
    :return: https://statsapi.web.nhl.com/api/v1/people/[playerid]
    """
    return 'https://statsapi.web.nhl.com/api/v1/people/{0:s}'.format(str(playerid))


def get_season_schedule_filename(season):
    """
    Gets the filename for the season's schedule file
    :param season: int, the season
    :return: /scrape/data/other/[season]_schedule.feather
    """
    return os.path.join(get_other_data_folder(), '{0:d}_schedule.feather'.format(season))


def get_season_schedule(season):
    """
    Gets the the season's schedule file. Stored as a feather file for fast read/write
    :param season: int, the season
    :return: file from /scrape/data/other/[season]_schedule.feather
    """
    return _SCHEDULES[season]


def _get_season_schedule(season):
    """
    Gets the the season's schedule file. Stored as a feather file for fast read/write
    :param season: int, the season
    :return: file from /scrape/data/other/[season]_schedule.feather
    """
    return feather.read_dataframe(get_season_schedule_filename(season))


def try_to_access_dict(base_dct, *keys, **kwargs):
    """
    A helper method that accesses base_dct using keys, one-by-one. Returns None if a key does not exist.
    :param base_dct: dict, a dictionary
    :param keys: str, int, or other valid dict keys
    :param kwargs: can specify default using kwarg default_return=0, for example.
    :return: base_dct[key1][key2][key3]... or None if a key is not in the dictionary
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


def generate_season_schedule_file(season, force_overwrite=True):
    """
    Reads season schedule from NHL API and writes to file.

    The output contains the following columns:
    - Season: int, the season
    - Date: str, the dates
    - Game: int, the game id
    - Type: str, the game type (for preseason vs regular season, etc)
    - Status: str, e.g. Final
    - Road: int, the road team ID
    - RoadScore: int, number of road team goals
    - RoadCoach str, 'N/A' when this function is run (edited later with road coach name)
    - Home: int, the home team ID
    - HomeScore: int, number of home team goals
    - HomeCoach: str, 'N/A' when this function is run (edited later with home coach name)
    - Venue: str, the name of the arena
    - Result: str, 'N/A' when this function is run (edited accordingly later from PoV of home team: W, OTW, SOL, etc)
    - PBPStatus: str, 'Not scraped' when this function is run (edited accordingly later)
    - TOIStatus: str, 'Not scraped' when this function is run (edited accordingly later)
    :param season: int, the season
    :param force_overwrite: bool. If True, generates entire file from scratch.
    If False, only redoes when not Final previously.'
    :return: Nothing
    """
    _print_and_log('Generating season schedule for {0:d}'.format(season))
    url = get_season_schedule_url(season)
    with urllib.request.urlopen(url) as reader:
        page = reader.read()

    dates = []
    games = []
    gametypes = []
    statuses = []
    vids = []
    vscores = []
    hids = []
    hscores = []
    venues = []

    page2 = json.loads(page)
    for datejson in page2['dates']:
        try:
            date = try_to_access_dict(datejson, 'date')
            for gamejson in datejson['games']:
                game = int(str(try_to_access_dict(gamejson, 'gamePk'))[-5:])
                gametype = try_to_access_dict(gamejson, 'gameType')
                status = try_to_access_dict(gamejson, 'status', 'detailedState')
                vid = try_to_access_dict(gamejson, 'teams', 'away', 'team', 'id')
                vscore = int(try_to_access_dict(gamejson, 'teams', 'away', 'score'))
                hid = try_to_access_dict(gamejson, 'teams', 'home', 'team', 'id')
                hscore = int(try_to_access_dict(gamejson, 'teams', 'home', 'score'))
                venue = try_to_access_dict(gamejson, 'venue', 'name')

                dates.append(date)
                games.append(game)
                gametypes.append(gametype)
                statuses.append(status)
                vids.append(vid)
                vscores.append(vscore)
                hids.append(hid)
                hscores.append(hscore)
                venues.append(venue)
        except KeyError:
            pass
    df = pd.DataFrame({'Date': dates,
                       'Game': games,
                       'Type': gametypes,
                       'Status': statuses,
                       'Road': vids,
                       'RoadScore': vscores,
                       'Home': hids,
                       'HomeScore': hscores,
                       'Venue': venues})

    df.loc[:, 'Season'] = season

    # Last step: we fill in some info from the pbp. If current schedule already exists, fill in that info.
    if os.path.exists(get_season_schedule_filename(season)):
        # only final games--this way pbp status and toistatus will be ok.
        cur_season = get_season_schedule(season).query('Status == "Final"')
        cur_season = cur_season[['Season', 'Game', 'HomeCoach', 'RoadCoach', 'Result', 'PBPStatus', 'TOIStatus']]
        df = df.merge(cur_season, how='left', on=['Season', 'Game'])

        # Fill in NAs
        df.loc[:, 'Season'] = df.Season.fillna(season)
        df.loc[:, 'HomeCoach'] = df.HomeCoach.fillna('N/A')
        df.loc[:, 'RoadCoach'] = df.RoadCoach.fillna('N/A')
        df.loc[:, 'Result'] = df.Result.fillna('N/A')
        df.loc[:, 'PBPStatus'] = df.PBPStatus.fillna('Not scraped')
        df.loc[:, 'TOIStatus'] = df.TOIStatus.fillna('Not scraped')
    else:
        df.loc[:, 'HomeCoach'] = 'N/A'  # Tried to set this to None earlier, but Arrow couldn't handle it, so 'N/A'
        df.loc[:, 'RoadCoach'] = 'N/A'
        df.loc[:, 'Result'] = 'N/A'
        df.loc[:, 'PBPStatus'] = 'Not scraped'
        df.loc[:, 'TOIStatus'] = 'Not scraped'

    _write_season_schedule(df, season, force_overwrite)

    _print_and_log('Done generating schedule for {0:d}'.format(season))
    

def update_schedule_with_pbp_scrape(season, game):
    """
    Updates the schedule file saying that specified game's pbp has been scraped.
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """
    df = get_season_schedule(season)
    df.loc[df.Game == game, "PBPStatus"] = "Scraped"
    _write_season_schedule(df, season, True)
    global _SCHEDULES
    _SCHEDULES[season] = df


def update_schedule_with_toi_scrape(season, game):
    """
    Updates the schedule file saying that specified game's toi has been scraped.
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """
    df = get_season_schedule(season)
    df.loc[df.Game == game, "TOIStatus"] = "Scraped"
    _write_season_schedule(df, season, True)
    global _SCHEDULES
    _SCHEDULES[season] = df


def _write_season_schedule(df, season, force_overwrite):
    """
    A helper method that writes the season schedule file to disk (in feather format for fast read/write)
    :param df: the season schedule datafraome
    :param season: the season
    :param force_overwrite: bool. If True, overwrites entire file.
    If False, only redoes when not Final previously.'
    :return: Nothing
    """
    if force_overwrite:  # Easy--just write it
        feather.write_dataframe(df, get_season_schedule_filename(season))
    else:  # Only write new games/previously unfinished games
        olddf = get_season_schedule(season)
        olddf = olddf.query('Status != "Final"')

        # TODO: Maybe in the future set status for games partially scraped as "partial" or something

        game_diff = set(df.Game).difference(olddf.Game)
        where_diff = df.Key.isin(game_diff)
        newdf = pd.concat(olddf, df[where_diff], ignore_index=True)

        feather.write_dataframe(newdf, get_season_schedule_filename(season))


def get_player_ids_filename():
    return os.path.join(get_other_data_folder(), 'PLAYER_INFO.feather')


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
        _print_and_log('Warning: overwriting existing player log with default, one-line df!', 'warn')
    write_player_log_file(df)


def write_player_log_file(df):
    """
    Writes the given dataframe to file as the player log filename
    :param df: pandas dataframe
    :return: nothing
    """
    feather.write_dataframe(df, get_player_log_filename())


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


def get_player_log_filename():
    """
    Returns the player log filename.
    :return: str, /scrape/data/other/PLAYER_LOG.feather
    """
    return os.path.join(get_other_data_folder(), 'PLAYER_LOG.feather')


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
        info[key] = try_to_access_dict(data, *val)

    # Remove the space in the middle of height
    if info['Height'] is not None:
        info['Height'] = info['Height'].replace(' ', '')
    return info


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
    global _PLAYERS
    _PLAYERS = _get_player_ids_file()
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
    if isinstance(seasons, int) or isinstance(games, np.int64):
        seasons = [seasons for _ in range(len(playerids))]
    if isinstance(games, int) or isinstance(games, np.int64):
        games = [games for _ in range(len(playerids))]
    if isinstance(teams, int) or isinstance(teams, str) or isinstance(teams, np.int64):
        teams = team_as_id(teams)
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
    global _PLAYER_LOG
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


def team_as_id(team):
    """
    A helper method. If team entered is int, returns that. If team is str, returns integer id of that team.
    :param team: int, or str
    :return: int, the team ID
    """
    if isinstance(team, int) or isinstance(team, np.int64):
        return team
    elif isinstance(team, str):
        df = get_team_info_file().query('Team == "{0:s}" | Abbreviation == "{0:s}"'.format(team))
        if len(df) == 0:
            _print_and_log('Could not find ID for {0:s}'.format(team), 'warn')
            return None
        elif len(df) == 1:
            return df.ID.iloc[0]
        else:
            _print_and_log('Multiple results when searching for {0:s}; returning first result'.format(team), 'warn')
            _print_and_log(df.to_string(), 'info')
            return df.ID.iloc[0]
    else:
        _print_and_log('Specified wrong type for team: {0:s}'.format(type(team)), 'warn')
        return None


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
    elif isinstance(team, int) or isinstance(team, np.int64):
        df = get_team_info_file().query('ID == {0:d}'.format(team))
        if len(df) == 0:
            _print_and_log('Could not find name for {0:d}'.format(team), 'warn')
            return None
        elif len(df) == 1:
            return df[col_to_access].iloc[0]
        else:
            _print_and_log('Multiple results when searching for {0:d}; returning first result'.format(team), 'warn')
            _print_and_log(df.to_string(), 'warn')
            return df[col_to_access].iloc[0]
    else:
        _print_and_log('Specified wrong type for team: {0:s}'.format(type(team)), 'warn')
        return None
    
    
def player_as_id(player):
    """
    A helper method. If player entered is int, returns that. If player is str, returns integer id of that player.
    :param player: int, or str
    :return: int, the player ID
    """
    if isinstance(player, int) or isinstance(player, np.int64):
        return player
    elif isinstance(player, str):
        df = get_player_ids_file().query('Name == "{0:s}"'.format(player))
        if len(df) == 0:
            _print_and_log('Could not find exact match for for {0:s}; trying exact substring match'.format(player))
            df = get_player_ids_file()
            df = df[df.Name.str.contains(player)]
            if len(df) == 0:
                _print_and_log('Could not find exact substring match; trying fuzzy matching')
                # TODO fuzzy match
                return None
            elif len(df) == 1:
                return df.ID.iloc[0]
            else:
                _print_and_log('Multiple results when searching for {0:s}; returning first result'.format(player),
                               'warn')
                _print_and_log(df.to_string(), 'warn')
                return df.ID.iloc[0]
        elif len(df) == 1:
            return df.ID.iloc[0]
        else:
            _print_and_log('Multiple results when searching for {0:s}; returning first result'.format(player), 'warn')
            _print_and_log(df.to_string(), 'warn')
            return df.ID.iloc[0]
    else:
        _print_and_log('Specified wrong type for player: {0:s}'.format(type(player)), 'warn')
        return None


def player_as_str(player):
    """
    A helper method. If player entered is str, returns that. If player is int, returns string name of that player.
    :param player: int, or str
    :return: str, the player name
    """
    if isinstance(player, str):
        return player
    elif isinstance(player, int) or isinstance(player, np.int64):
        df = get_player_ids_file().query('ID == {0:d}'.format(player))
        if len(df) == 0:
            _print_and_log('Could not find name for {0:d}'.format(player), 'warn')
            return None
        elif len(df) == 1:
            return df.Name.iloc[0]
        else:
            _print_and_log('Multiple results when searching for {0:d}; returning first result'.format(player), 'warn')
            _print_and_log(df.to_string(), 'warn')
            return df.Name.iloc[0]
    else:
        _print_and_log('Specified wrong type for team: {0:d}'.format(type(player)), 'warn')
        return None


def refresh_schedules():
    """
    Reloads schedules to memory. Use this after updating schedule file on disk, because get_season_schedule()
    loads from memory.
    :return: nothing
    """
    global _SCHEDULES
    _SCHEDULES = {season: _get_season_schedule(season) for season in range(2005, _CURRENT_SEASON + 1)}


def get_game_data_from_schedule(season, game):
    """
    This is a helper method that uses the schedule file to isolate information for current game
    (e.g. teams involved, coaches, venue, score, etc.)
    :param season: int, the season
    :param game: int, the game
    :return: dict of game data
    """

    schedule_item = get_season_schedule(season).query('Game == {0:d}'.format(game)).to_dict(orient='series')
    # The output format of above was {colname: np.array[vals]}. Change to {colname: val}
    schedule_item = {k: v.values[0] for k, v in schedule_item.items()}
    return schedule_item


def update_schedule_with_coaches(season, game, homecoach, roadcoach):
    """
    Updates the season schedule file with given coaches' names (which are listed 'N/A' at schedule generation)
    :param season: int, the season
    :param game: int, the game
    :param homecoach: str, the home coach name
    :param roadcoach: str, the road coach name
    :return:
    """

    # Replace coaches with N/A if None b/c feather has trouble with mixed datatypes. Need str here.
    if homecoach is None:
        homecoach = 'N/A'
    if roadcoach is None:
        roadcoach = 'N/A'

    # Edit relevant schedule files
    df = get_season_schedule(season)
    df.loc[df.Game == game, 'HomeCoach'] = homecoach
    df.loc[df.Game == game, 'RoadCoach'] = roadcoach

    # Write to file and refresh schedule in memory
    _write_season_schedule(df, season, True)
    refresh_schedules()


def update_schedule_with_result(season, game, result):
    """
    Updates the season schedule file with game result (which are listed 'N/A' at schedule generation)
    :param season: int, the season
    :param game: int, the game
    :param result: str, the result from home team perspective
    :return:
    """

    # Replace coaches with N/A if None b/c feather has trouble with mixed datatypes. Need str here.
    if result is None:
        result = 'N/A'

    # Edit relevant schedule files
    df = get_season_schedule(season)
    df.loc[df.Game == game, 'Result'] = result

    # Write to file and refresh schedule in memory
    _write_season_schedule(df, season, True)
    refresh_schedules()

logging.basicConfig(level=logging.DEBUG, filemode="a+",
                    format="%(asctime)-15s %(levelname)-8s %(message)s",
                    filename = 'logfile.log')

_CURRENT_SEASON = _get_current_season()
_BASE_DIR = _get_base_dir()
_create_folders_and_files()
_TEAMS = _get_team_info_file()
_PLAYERS = _get_player_ids_file()
_PLAYER_LOG = _get_player_log_file()
_SCHEDULES = {season: _get_season_schedule(season) for season in range(2005, _CURRENT_SEASON + 1)}
