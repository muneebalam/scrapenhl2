"""
This module contains methods related to season schedules.
"""

import datetime
import functools
import json
import os.path
import urllib.request

import feather
import pandas as pd

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.team_info as team_info


def _get_current_season():
    """
    Runs at import only. Sets current season as today's year minus 1, or today's year if it's September or later

    :return: int, current season
    """
    date = datetime.datetime.now()
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


def get_season_schedule_filename(season):
    """
    Gets the filename for the season's schedule file

    :param season: int, the season

    :return: str, /scrape/data/other/[season]_schedule.feather
    """
    return os.path.join(organization.get_other_data_folder(), '{0:d}_schedule.feather'.format(season))


def get_season_schedule(season):
    """
    Gets the the season's schedule file from memory.

    :param season: int, the season

    :return: dataframe (originally from /scrape/data/other/[season]_schedule.feather)
    """
    return _SCHEDULES[season]


def get_team_schedule(season, team):
    """
    Gets the schedule for given team in given season.

    :param season: int, the season
    :param team: int or str, the team

    :return: dataframe
    """
    df = get_season_schedule(season)
    tid = team_info.team_as_id(team)
    return df[(df.Home == tid) | (df.Road == tid)]


def get_team_games(season, team):
    """
    Returns list of games played by team in season.

    :param season: int, the season
    :param team: int or str, the team

    :return: series of games
    """
    return get_team_schedule(season, team).Game


def _get_season_schedule(season):
    """
    Gets the the season's schedule file. Stored as a feather file for fast read/write

    :param season: int, the season

    :return: dataframe from /scrape/data/other/[season]_schedule.feather
    """
    return feather.read_dataframe(get_season_schedule_filename(season))


def write_season_schedule(df, season, force_overwrite):
    """
    A helper method that writes the season schedule file to disk (in feather format for fast read/write)

    :param df: the season schedule datafraome
    :param season: the season
    :param force_overwrite: bool. If True, overwrites entire file. If False, only redoes when not Final previously.

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
    schedule_setup()


@functools.lru_cache(maxsize=128, typed=False)
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


def schedule_setup():
    """
    Reads current season and schedules into memory.

    :return: nothing
    """
    global _SCHEDULES, _CURRENT_SEASON
    _CURRENT_SEASON = _get_current_season()
    for season in range(2005, get_current_season() + 1):
        if not os.path.exists(get_season_schedule_filename(season)):
            generate_season_schedule_file(season)  # season schedule
            # There is a potential issue here for current season.
            # For current season, we'll update this as we go along.
            # But original creation first time you start up in a new season is automatic, here.
            # When we autoupdate season date, we need to make sure to re-access this file and add in new entries
    _SCHEDULES = {season: _get_season_schedule(season) for season in range(2005, _CURRENT_SEASON + 1)}


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
    page = helpers.try_url_n_times(get_season_schedule_url(season))

    page2 = json.loads(page.decode('latin-1'))
    df = _create_schedule_dataframe_from_json(page2)
    df.loc[:, 'Season'] = season

    # Last step: we fill in some info from the pbp. If current schedule already exists, fill in that info.
    df = _fill_in_schedule_from_pbp(df, season)
    write_season_schedule(df, season, force_overwrite)


def _create_schedule_dataframe_from_json(jsondict):
    """
    Reads game, game type, status, visitor ID, home ID, visitor score, and home score for each game in this dict

    :param jsondict: a dictionary formed from season schedule json

    :return: pandas dataframe
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
            date = helpers.try_to_access_dict(datejson, 'date')
            for gamejson in datejson['games']:
                game = int(str(helpers.try_to_access_dict(gamejson, 'gamePk'))[-5:])
                gametype = helpers.try_to_access_dict(gamejson, 'gameType')
                status = helpers.try_to_access_dict(gamejson, 'status', 'detailedState')
                vid = helpers.try_to_access_dict(gamejson, 'teams', 'away', 'team', 'id')
                vscore = int(helpers.try_to_access_dict(gamejson, 'teams', 'away', 'score'))
                hid = helpers.try_to_access_dict(gamejson, 'teams', 'home', 'team', 'id')
                hscore = int(helpers.try_to_access_dict(gamejson, 'teams', 'home', 'score'))
                venue = helpers.try_to_access_dict(gamejson, 'venue', 'name')

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
                       'Venue': venues}).sort_values('Game')
    return df


def _fill_in_schedule_from_pbp(df, season):
    """
    Fills in columns for coaches, result, pbp status, and toi status as N/A, not scraped, etc.
    Use methods prefixed with update_schedule to actually fill in with correct values.

    :param df: dataframe, season schedule dataframe as created by _create_schedule_dataframe_from_json
    :param season: int, the season

    :return: df, with coaches, result, and status filled in
    """

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
    return df


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
_SCHEDULES = None
schedule_setup()
