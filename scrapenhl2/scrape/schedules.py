"""
This module contains methods related to season schedules.
"""

import arrow
import datetime
import functools
import json
import os.path
import urllib.request

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

    :return: connection
    """
    return sqlite3.connect(get_schedule_filename())


def close_schedule_cursor():
    """
    Close cursor for schedule

    :return:
    """

    _SCH_CONN.close()


def get_season_schedule(season):
    """
    Gets the season's schedule file from SQL.
    
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

    :return: dataframe 
    """
    query = 'SELECT * FROM Schedule WHERE Season = {0:d}'.format(season)
    return pd.read_sql_query(query, _SCH_CONN)


def get_schedule(*colnames):
    """
    Gets the schedule file from SQL.
    
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

    :param colnames: str, column names

    :return: dataframe
    """
    if len(colnames) == 0:
        colnames = ['*']
    colnames2 = ', '.join(colnames)
    return pd.read_sql_query('SELECT {0:s} FROM Schedule'.format(colnames2), _SCH_CONN)


def _get_schedule_table_colnames_coltypes():
    """
    Returns schedule column names and types (hard coded)

    - Season INT PRIMARY KEY
    - Date DATE
    - Game INT PRIMARY KEY
    - Home INT
    - HomeScore INT
    - Road INT
    - RoadScore INT
    - Status CHAR
    - Type CHAR
    - Venue CHAR
    - HomeCoach CHAR DEFAULT N/A
    - RoadCoach CHAR DEFAULT N/A
    - Result CHAR DEFAULT N/A
    - PBPStatus CHAR DEFAULT Not scraped
    - TOIStatus CHAR DEFAULT Not scraped

    :return: list
    """

    return [('Season', 'INT'), ('Date', 'DATE'), ('Game', 'INT'),
            ('Home', 'INT'), ('HomeScore', 'INT'), ('Road', 'INT'), ('RoadScore', 'INT'),
            ('Status', 'CHAR'), ('Type', 'CHAR(2)'), ('Venue', 'CHAR'),
            ('HomeCoach', 'CHAR', 'DEFAULT', '"N/A"'),
            ('RoadCoach', 'CHAR', 'DEFAULT', '"N/A"'),
            ('Result', 'CHAR', 'DEFAULT', '"N/A"'),
            ('PBPStatus', 'CHAR', 'DEFAULT', '"Not scraped"'),
            ('TOIStatus', 'CHAR', 'DEFAULT', '"Not scraped"')]


def _create_schedule_table():
    """
    Creates table with primary keys Season and Game, and columns Date, Home, HomeScore, Road, RoadScore,
    Status, Type, Venue, HomeCoach, RoadCoach, Result, PBPStatus, and TOIStatus

    :return:
    """
    cols = _get_schedule_table_colnames_coltypes()
    cols = ',\n'.join([' '.join(row) for row in cols])
    query = 'CREATE TABLE Schedule (\n{0:s},\nPRIMARY KEY ({1:s}, {2:s}))'.format(cols, 'Season', 'Game')
    _SCH_CURSOR.execute(query)
    _SCH_CONN.commit()


def write_schedules():
    """
    Writes all season schedules to DB

    :return:
    """
    fname = get_schedule_filename()
    try:
        sch = get_season_schedule(get_current_season())
    except pd.io.sql.DatabaseError:
        _create_schedule_table()

    for season in range(2005, get_current_season()):
        sch = get_season_schedule(season)
        if len(sch) == 0:
            generate_season_schedule_file(season)


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
    
    query = 'SELECT * FROM Schedule'
    
    filters = []
    if season is not None:
        filters.append('Season = {0:d}'.format(season))
    if team is not None:
        filters.append('Team = {0:d}'.format(int(team_info.team_as_id(team))))
    if startdate is not None:
        filters.append('Date >= {0:s}'.format(startdate))
    if enddate is not None:
        filters.append('Date <= {0:s}'.format(enddate))
        
    filters = ' AND '.join(filters)
    if len(filters) > 0:
        query = '{0:s} WHERE {1:s}'.format(query, filters)
    
    return pd.read_sql_query(query, _SCH_CONN)


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
def get_game_data_from_schedule(season, game, *colnames):
    """
    This is a helper method that uses the schedule file to isolate information for current game
    (e.g. teams involved, coaches, venue, score, etc.)

    :param season: int, the season
    :param game: int, the game
    :param colnames: str, column names. If none, gets all

    :return: dict of game data
    """

    if len(colnames) == 0:
        cols = '*'
    else:
        cols = ', '.join(list(colnames))

    query = 'SELECT {0:s} FROM Schedule WHERE Season = {1:d} AND Game = {2:d}'.format(cols, season, game)
    schedule_item = pd.read_sql_query(query, _SCH_CONN).to_dict(orient='series')
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
    return get_game_data_from_schedule(season, game, 'Date')['Date']


def get_home_team(season, game, returntype='id'):
    """
    Returns the home team from this game

    :param season: int, the game
    :param game: int, the season
    :param returntype: str, 'id' or 'name'

    :return: float or str, depending on returntype
    """
    home = get_game_data_from_schedule(season, game, 'Home')['Home']
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
    road = get_game_data_from_schedule(season, game, 'Road')['Road']
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
    return get_game_data_from_schedule(season, game, 'HomeScore')['HomeScore']


def get_road_score(season, game):
    """
    Returns the road score from this game

    :param season: int, the season
    :param game: int, the game

    :return: int, the score
    """
    return get_game_data_from_schedule(season, game, 'RoadScore')['RoadScore']


def get_game_status(season, game):
    """
    Returns the status of this game (e.g. Final, In Progress)

    :param season: int, the season
    :param game: int, the game

    :return: int, the score
    """
    return get_game_data_from_schedule(season, game, 'Status')['Status']


def get_game_result(season, game):
    """
    Returns the result of this game for home team (e.g. W, SOL)

    :param season: int, the season
    :param game: int, the game

    :return: int, the score
    """
    return get_game_data_from_schedule(season, game, 'Result')['Result']


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
    Checks if gameid in schedule.

    :param season: int, season
    :param game: int, game
    :return: bool
    """

    return len(pd.read_sql_query('SELECT * FROM Schedule WHERE Season = {0:d} AND Game = {1:d}'.format(season, game),
                                 _SCH_CONN)) == 1


def schedule_setup():
    """
    Reads current season and schedules into memory.

    :return: nothing
    """
    clear_caches()
    global _SCH_CURSOR, _SCH_CONN, _CURRENT_SEASON
    _CURRENT_SEASON = _get_current_season()
    _SCH_CONN = get_schedule_connection()
    _SCH_CURSOR = _SCH_CONN.cursor()


def generate_season_schedule_file(season):
    """
    Reads season schedule from NHL API and writes to db.

    :param season: int, the season

    :return: Nothing
    """

    page = helpers.try_url_n_times(get_season_schedule_url(season))
    if page is None:
        print('Schedule for {0:d}-{1:s} was None'.format(season, str(season + 1)[2:]))
        return

    page2 = json.loads(page)
    _add_schedule_from_json(season, page2)
    print('Wrote {0:d}-{1:s} schedule to file'.format(season, str(season + 1)[2:]))
    clear_caches()


def _add_schedule_from_json(season, jsondict):
    """
    Reads game, game type, status, visitor ID, home ID, visitor score, and home score for each game in this dict.

    Adds to SQL

    :param season: int
    :param jsondict: a dictionary formed from season schedule json

    :return:
    """
    for datejson in jsondict['dates']:
        try:
            date = datejson.get('date', None)
            for gamejson in datejson['games']:
                _update_schedule(Season=season, Date=date,
                                 Game=int(str(helpers.try_to_access_dict(gamejson, 'gamePk'))[-5:]),
                                 Type=helpers.try_to_access_dict(gamejson, 'gameType'),
                                 Home=helpers.try_to_access_dict(gamejson, 'teams', 'home', 'team', 'id'),
                                 Road=helpers.try_to_access_dict(gamejson, 'teams', 'away', 'team', 'id'),
                                 HomeScore=int(helpers.try_to_access_dict(gamejson, 'teams', 'home', 'score')),
                                 RoadScore=int(helpers.try_to_access_dict(gamejson, 'teams', 'away', 'score')),
                                 Status=helpers.try_to_access_dict(gamejson, 'status', 'detailedState'),
                                 Venue=helpers.try_to_access_dict(gamejson, 'venue', 'name'))

        except KeyError:
            pass

    _SCH_CONN.commit()


def _update_schedule(**kwargs):
    """
    Updates schedule using REPLACE INTO. DOES NOT COMMIT.

    :param kwargs:
    :return:
    """
    helpers._update_table(_SCH_CURSOR, 'Schedule', **kwargs)


def attach_game_dates_to_dateframe(df):
    """
    Takes dataframe with Season and Game columns and adds a Date column (for that game)

    :param df: dataframe

    :return: dataframe with one more column
    """
    return df.merge(get_schedule('Season', 'Game', 'Date'), how='left', on=['Season', 'Game'])


_CURRENT_SEASON = None
_SCH_CONN = None
_SCH_CURSOR = None
schedule_setup()
