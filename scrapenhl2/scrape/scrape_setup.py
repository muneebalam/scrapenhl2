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

def _get_current_season():
    """
    Runs at import only. Sets current season as today's year minus 1, or today's year if it's September or later
    :return: int, current season
    """
    season = datetime.datetime.now().year - 1
    if datetime.datetime.now().month >= 9:
        season += 1
    return(season)


def get_current_season():
    """
    :return: The current season variable (generated at import from _get_current_season)
    """
    return _CURRENT_SEASON


def _get_base_dir():
    """
    Returns the base directory (two directories up from __file__)
    :return: the base directory
    """
    return(os.path.join(*__file__.split('/')[:-2]))


def get_base_dir():
    """
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
        /scrape/data/raw/pbp/[seasons]/
        /scrape/data/raw/toi/[seasons]/
        /scrape/data/parsed/pbp/[seasons]/
        /scrape/data/parsed/toi/[seasons]/
        /scrape/data/teams/pbp/[seasons]/
        /scrape/data/teams/toi/[seasons]/
        /scrape/data/other/
    Also creates team IDs file and season schedule files if they don't exist already.
    :return: Nothing
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


def get_game_raw_pbp_filename(season, game):
    """
    Returns the filename of the raw pbp folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/raw/pbp/[season]/[game].JSON
    """
    return os.path.join(get_season_raw_pbp_folder(season), str(game) + '.json')


def get_game_raw_toi_filename(season, game):
    """
    Returns the filename of the raw toi folder
    :param season: int, current season
    :param game: int, game
    :return:  /scrape/data/raw/toi/[season]/[game].JSON
    """
    return os.path.join(get_season_raw_toi_folder(season), str(game) + '.json')


def get_game_parsed_pbp_filename(season, game):
    """
    Returns the filename of the parsed pbp folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/parsed/pbp/[season]/[game].zlib
    """
    return os.path.join(get_season_parsed_pbp_folder(season), str(game) + '.zlib')


def get_game_parsed_toi_filename(season, game):
    """
    Returns the filename of the parsed toi folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/parsed/toi/[season]/[game].zlib
    """
    return os.path.join(get_season_parsed_toi_folder(season), str(game) + '.zlib')


def get_raw_data_folder():
    """
    Returns the folder containing raw data
    :return: /scrape/data/raw/
    """
    return os.path.join('scrape', 'data', 'raw')


def get_parsed_data_folder():
    """
    Returns the folder containing parsed data
    :return: /scrape/data/parsed/
    """
    return os.path.join('scrape', 'data', 'parsed')


def get_team_data_folder():
    """
    Returns the folder containing team log data
    :return: /scrape/data/teams/
    """
    return os.path.join('scrape', 'data', 'teams')


def get_other_data_folder():
    """
    Returns the folder containing other data
    :return: /scrape/data/other/
    """
    return os.path.join('scrape', 'data', 'other')


def get_season_raw_pbp_folder(season):
    """
    Returns the folder containing raw pbp for given season
    :param: season: int, current season
    :return: /scrape/data/raw/pbp/[season]/
    """
    return os.path.join(get_raw_data_folder(), 'pbp', str(season))


def get_season_raw_toi_folder(season):
    """
    Returns the folder containing raw toi for given season
    :param: season: int, current season
    :return: /scrape/data/raw/toi/[season]/
    """
    return os.path.join(get_raw_data_folder(), 'toi', str(season))


def get_season_parsed_pbp_folder(season):
    """
    Returns the folder containing parsed pbp for given season
    :param: season: int, current season
    :return: /scrape/data/parsed/pbp/[season]/
    """
    return os.path.join(get_parsed_data_folder(), 'pbp', str(season))


def get_season_parsed_toi_folder(season):
    """
    Returns the folder containing parsed toi for given season
    :param: season: int, current season
    :return: /scrape/data/raw/toi/[season]/
    """
    return os.path.join(get_parsed_data_folder(), 'toi', str(season))


def get_season_team_pbp_folder(season):
    """
    Returns the folder containing team pbp logs for given season
    :param: season: int, current season
    :return: /scrape/data/teams/pbp/[season]/
    """
    return os.path.join(get_team_data_folder(), 'pbp', str(season))


def get_season_team_toi_folder(season):
    """
    Returns the folder containing team toi logs for given season
    :param: season: int, current season
    :return: /scrape/data/teams/toi/[season]/
    """
    return os.path.join(get_team_data_folder(), 'toi', str(season))


def get_team_info_filename():
    """
    Returns the team information filename
    :return: /scrape/data/other/TEAM_INFO.feather
    """
    return os.path.join(get_other_data_folder(), 'TEAM_INFO.feather')


def get_team_info_file():
    """
    Returns the team information file. This is stored as a feather file for fast read/write.
    :return: file from /scrape/data/other/TEAM_INFO.feather
    """
    return feather.read_dataframe(get_team_info_filename())


def write_team_info_file(df):
    """
    Writes the team information file. This is stored as a feather file for fast read/write.
    :param : df: the (team information) dataframe to write to file
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
    Reads all team id URLs and stores information to disk
    :param limit: int. Tries every team ID from 1 to limit (inclusive)
    :return: nothing
    """
    print('Creating team IDs file')
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

            print('Done with ID #', tid, ':', tname)

        except urllib.error.HTTPError:
            pass

    teaminfo = pd.DataFrame({'ID': ids, 'Abbreviation': abbrevs, 'Name': names})
    write_team_info_file(teaminfo)
    print('Done writing team IDs')


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
    return feather.read_dataframe(get_season_schedule_filename(season))


def _try_to_access_dict(base_dct, *keys):
    """
    A helper method that accesses base_dct using keys, one-by-one. Returns None if a key does not exist.
    :param base_dct: dict, a dictionary
    :param keys: str, int, or other valid dict keys
    :return: base_dct[key1][key2][key3]... or None if a key is not in the dictionary
    """
    temp = base_dct
    try:
        for key in keys:
            temp = temp[key]
        return temp
    except KeyError:
        return None


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
    print('Generating season schedule for', season)
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
            date = _try_to_access_dict(datejson, 'date')
            for gamejson in datejson['games']:
                game = _try_to_access_dict(gamejson, 'gamePk')
                gametype = _try_to_access_dict(gamejson, 'gameType')
                status = _try_to_access_dict(gamejson, 'status', 'detailedState')
                vid = _try_to_access_dict(gamejson, 'teams', 'away', 'team', 'id')
                vscore = _try_to_access_dict(gamejson, 'teams', 'away', 'score')
                hid = _try_to_access_dict(gamejson, 'teams', 'home', 'team', 'id')
                hscore = _try_to_access_dict(gamejson, 'teams', 'home', 'score')
                venue = _try_to_access_dict(gamejson, 'venue', 'name')

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
    df.loc[:, 'HomeCoach'] = 'N/A'  # Tried to set this to None earlier, but Arrow couldn't handle it, so 'N/A' it is
    df.loc[:, 'RoadCoach'] = 'N/A'
    df.loc[:, 'Result'] = 'N/A'
    df.loc[:, 'PBPStatus'] = 'Not scraped'
    df.loc[:, 'TOIStatus'] = 'Not scraped'

    _write_season_schedule(df, season, force_overwrite)

    print('Done generating schedule for', season)


def _write_season_schedule(df, season, force_overwrite):
    """
    A helper method that writes the season schedule file to disk (in feather format for fast read/write)
    :param df: the season schedule datafraome
    :param season: the season
    :param force_overwrite: bool. If True, generates entire file from scratch.
    If False, only redoes when not Final previously.'
    :return: Nothing
    """
    if force_overwrite:  # Easy--just write it
        feather.write_dataframe(df, get_season_schedule_filename(season))
    else:  # Only write new games/previously unfinished games
        olddf = feather.read_dataframe(get_season_schedule_filename(season))
        olddf = olddf.query('Status != "Final"')

        # TODO: Maybe in the future set status for games partially scraped as "partial" or something

        game_diff = set(df.Game).difference(olddf.Game)
        where_diff = df.Key.isin(game_diff)
        newdf = pd.concat(olddf, df[where_diff], ignore_index=True)

        feather.write_dataframe(newdf, get_season_schedule_filename(season))


_CURRENT_SEASON = _get_current_season()
_BASE_DIR = _get_base_dir()
_create_folders_and_files()
