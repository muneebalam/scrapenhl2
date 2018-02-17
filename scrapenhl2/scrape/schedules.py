"""
This module contains methods related to season schedules.
"""

import arrow
import datetime
import functools
import json
import os.path
import urllib.request

import feather
import pandas as pd
import sqlite3

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.team_info as team_info


def _get_current_season():
    """
    Runs at import only. Sets current season as today's year minus 1, or today's year if it's September or later

    :return: int, current season
    """
    date = arrow.now()
    season = date.year - 1
    if date.month >= 9:
        season += 1
    return season


def get_current_season():
    """
    Returns the current season.

    :return: The current season variable (generated at import from _get_current_season)
    """
    return _CURRENT_SEASON


def get_schedule_filename():
    """
    Returns SQL filename for schedule

    :return: str
    """
    return os.path.join(organization.get_other_data_folder(), "schedule.sqlite")


def get_schedule_connection():
    """
    Get connection and cursor for schedule

    :return: cursor
    """
    return sqlite3.connect(get_schedule_filename())


def close_schedule_cursor():
    """
    Close cursor for schedule

    :return:
    """

    _CONNECTION.close()


def get_season_schedule(season):
    """
    Gets the the season's schedule file from SQL.

    :param season: int, the season

    :return: dataframe (originally from /scrape/data/other/[season]_schedule.feather)
    """
    query = 'SELECT * FROM Schedule WHERE Season = {0:d}'.format(season)
    return pd.read_sql_query(query, _CONNECTION)


def _get_schedule_table_colnames_coltypes():
    """
    Returns schedule column names and types (hard coded)

    :return: list
    """

    return [('Season', 'INT'), ('Date', 'DATE'), ('Game', 'INT'),
            ('Home', 'INT'), ('HomeScore', 'INT'), ('Road', 'INT'), ('RoadScore', 'INT'),
            ('Status', 'CHAR'), ('Type', 'CHAR(2)'), ('Venue', 'CHAR'),
            ('HomeCoach', 'CHAR', 'DEFAULT', '"N/A"'),
            ('RoadCoach', 'CHAR', 'DEFAULT', '"N/A"'),
            ('Result', 'CHAR', 'DEFAULT', '"N/A"'),
            ('PBPStatus', 'CHAR', 'DEFAULT', '"Not scraped"'),
            ('TOIStatus', 'CHAR', 'DEFAULT', '"N/A"')]


def write_schedules():
    """
    Writes all season schedules to DB

    :return:
    """
    fname = get_schedule_filename()
    try:
        sch = get_season_schedule(get_current_season())
    except pd.io.sql.DatabaseError:
        # Create the table
        # feather.read_dataframe('/Volumes/My Passport for Mac/scrapenhl2/scrapenhl2/data/other/2017_schedule.feather').head()
        cols = _get_schedule_table_colnames_coltypes()
        cols = ',\n'.join([' '.join(row) for row in cols])
        query = 'CREATE TABLE Schedule (\n{0:s},\nPRIMARY KEY ({1:s}, {2:s}))'.format(cols, 'Season', 'Game')
        _CURSOR.execute(query)
        _CONNECTION.commit()

    for season in range(2005, get_current_season()):
        sch = get_season_schedule(season)
        if len(sch) == 0:
            page = helpers.try_url_n_times(get_season_schedule_url(season))
            if page is None:
                print('Schedule for {0:d}-{1:s} was None'.format(season, str(season+1)[2:]))
                continue
            page2 = json.loads(page)
            _add_schedule_from_json(season, page2)
            print('Wrote {0:d}-{1:s} schedule to file'.format(season, str(season+1)[2:]))


def get_team_schedule(season=None, team=None, startdate=None, enddate=None):
    """
    Gets the schedule for given team in given season. Or if startdate and enddate are specified, searches between
    those dates. If season and startdate (and/or enddate) are specified, searches that season between those dates.

    :param season: int, the season
    :param team: int or str, the team
    :param startdate: str, YYYY-MM-DD
    :param enddate: str, YYYY-MM-DD

    :return: dataframe
    """
    # TODO handle case when only team and startdate, or only team and enddate, are given
    if season is not None:
        df = get_season_schedule(season).query('Status != "Scheduled"')
        if startdate is not None:
            df = df.query('Date >= "{0:s}"'.format(startdate))
        if enddate is not None:
            df = df.query('Date <= "{0:s}"'.format(enddate))
        tid = team_info.team_as_id(team)
        return df[(df.Home == tid) | (df.Road == tid)]
    if startdate is not None and enddate is not None:
        dflst = []
        startseason = helpers.infer_season_from_date(startdate)
        endseason = helpers.infer_season_from_date(enddate)
        for season in range(startseason, endseason + 1):
            df = get_team_schedule(season, team) \
                .query('Status != "Scheduled"') \
                .assign(Season=season)
            if season == startseason:
                df = df.query('Date >= "{0:s}"'.format(startdate))
            if season == endseason:
                df = df.query('Date <= "{0:s}"'.format(enddate))
            dflst.append(df)
        df = pd.concat(dflst)
        return df


def get_team_games(season=None, team=None, startdate=None, enddate=None):
    """
    Returns list of games played by team in season.

    Just calls get_team_schedule with the provided arguments, returning the series of games from that dataframe.

    :param season: int, the season
    :param team: int or str, the team
    :param startdate: str or None
    :param enddate: str or None

    :return: series of games
    """
    return get_team_schedule(season, team, startdate, enddate).Game


def clear_caches():
    """
    Clears caches for methods in this module.
    :return:
    """
    get_game_data_from_schedule.cache_clear()


@functools.lru_cache(maxsize=1024, typed=False)
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


def get_game_date(season, game):
    """
    Returns the date of this game

    :param season: int, the game
    :param game: int, the season

    :return: str
    """
    return get_game_data_from_schedule(season, game)['Date']


def get_home_team(season, game, returntype='id'):
    """
    Returns the home team from this game

    :param season: int, the game
    :param game: int, the season
    :param returntype: str, 'id' or 'name'

    :return: float or str, depending on returntype
    """
    home = get_game_data_from_schedule(season, game)['Home']
    if returntype.lower() == 'id':
        return team_info.team_as_id(home)
    else:
        return team_info.team_as_str(home)


def get_road_team(season, game, returntype='id'):
    """
    Returns the road team from this game

    :param season: int, the game
    :param game: int, the season
    :param returntype: str, 'id' or 'name'

    :return: float or str, depending on returntype
    """
    road = get_game_data_from_schedule(season, game)['Road']
    if returntype.lower() == 'id':
        return team_info.team_as_id(road)
    else:
        return team_info.team_as_str(road)


def get_home_score(season, game):
    """
    Returns the home score from this game

    :param season: int, the season
    :param game: int, the game

    :return: int, the score
    """
    return int(get_game_data_from_schedule(season, game)['HomeScore'])


def get_road_score(season, game):
    """
    Returns the road score from this game

    :param season: int, the season
    :param game: int, the game

    :return: int, the score
    """
    return int(get_game_data_from_schedule(season, game)['RoadScore'])


def get_game_status(season, game):
    """
    Returns the status of this game (e.g. Final, In Progress)

    :param season: int, the season
    :param game: int, the game

    :return: int, the score
    """
    return get_game_data_from_schedule(season, game)['Status']


def get_game_result(season, game):
    """
    Returns the result of this game for home team (e.g. W, SOL)

    :param season: int, the season
    :param game: int, the game

    :return: int, the score
    """
    return get_game_data_from_schedule(season, game)['Result']


def get_season_schedule_url(season):
    """
    Gets the url for a page containing all of this season's games (Sep 1 to Jun 26) from NHL API.

    :param season: int, the season

    :return: str, https://statsapi.web.nhl.com/api/v1/schedule?startDate=[season]-09-01&endDate=[season+1]-06-25
    """
    return 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=' \
           '{0:d}-09-01&endDate={1:d}-06-25'.format(season, season + 1)


def get_teams_in_season(season):
    """
    Returns all teams that have a game in the schedule for this season

    :param season: int, the season

    :return: set of team IDs
    """

    sch = get_season_schedule(season)
    allteams = set(sch.Road).union(sch.Home)
    return set(allteams)


def check_valid_game(season, game):
    """
    Checks if gameid in season schedule.

    :param season: int, season
    :param game: int, game
    :return: bool
    """

    try:
        get_game_status(season, game)
        return True
    except IndexError:
        return False


def schedule_setup():
    """
    Reads current season and schedules into memory.

    :return: nothing
    """
    clear_caches()
    global _CURSOR, _CONNECTION, _CURRENT_SEASON
    _CURRENT_SEASON = _get_current_season()
    _CONNECTION = get_schedule_connection()
    _CURSOR = _CONNECTION.cursor()
    #for season in range(2005, get_current_season() + 1):
    #    if not os.path.exists(get_season_schedule_filename(season)):
    #        generate_season_schedule_file(season)  # season schedule
            # There is a potential issue here for current season.
            # For current season, we'll update this as we go along.
            # But original creation first time you start up in a new season is automatic, here.
            # When we autoupdate season date, we need to make sure to re-access this file and add in new entries
    #_SCHEDULES = {season: _get_season_schedule(season) for season in range(2005, _CURRENT_SEASON + 1)}


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
        If False, only redoes when not Final previously.

    :return: Nothing
    """

    df.loc[:, 'Season'] = season

    # Last step: we fill in some info from the pbp. If current schedule already exists, fill in that info.
    df = _fill_in_schedule_from_pbp(df, season)
    write_season_schedule(df, season, force_overwrite)
    clear_caches()


def _add_schedule_from_json(season, jsondict):
    """
    Reads game, game type, status, visitor ID, home ID, visitor score, and home score for each game in this dict.

    Adds to SQL

    :param season: int
    :param jsondict: a dictionary formed from season schedule json

    :return:
    """
    dates = []
    games = []
    gametypes = []
    statuses = []
    vids = []
    vscores = []
    hids = []
    hscores = []
    venues = []
    for datejson in jsondict['dates']:
        try:
            date = datejson.get('date', None)
            for gamejson in datejson['games']:
                game = int(str(helpers.try_to_access_dict(gamejson, 'gamePk'))[-5:])
                gametype = helpers.try_to_access_dict(gamejson, 'gameType')
                status = helpers.try_to_access_dict(gamejson, 'status', 'detailedState')
                vid = helpers.try_to_access_dict(gamejson, 'teams', 'away', 'team', 'id')
                vscore = int(helpers.try_to_access_dict(gamejson, 'teams', 'away', 'score'))
                hid = helpers.try_to_access_dict(gamejson, 'teams', 'home', 'team', 'id')
                hscore = int(helpers.try_to_access_dict(gamejson, 'teams', 'home', 'score'))
                venue = helpers.try_to_access_dict(gamejson, 'venue', 'name')

                cols = ', '.join(['Season', 'Date', 'Game', 'Home', 'HomeScore', 'Road', 'RoadScore', 'Status'])
                vals = ', '.join(['"{0:s}"'.format(x) if isinstance(x, str) else str(x) \
                                  for x in [season, date, game, hid, hscore, vid, vscore, status]])
                query = 'INSERT INTO Schedule ({0:s})\nVALUES ({1:s})'.format(cols, vals)

                _CURSOR.execute(query)

        except KeyError:
            pass

    _CONNECTION.commit()


def attach_game_dates_to_dateframe(df):
    """
    Takes dataframe with Season and Game columns and adds a Date column (for that game)

    :param df: dataframe

    :return: dataframe with one more column
    """
    dflst = []
    for season in df.Season.unique():
        temp = df.query("Season == {0:d}".format(int(season))) \
            .merge(get_season_schedule(season)[['Game', 'Date']], how='left', on='Game')
        dflst.append(temp)
    df2 = pd.concat(dflst)
    return df2


_CURRENT_SEASON = None
_CONNECTION = None
_CURSOR = None
schedule_setup()
