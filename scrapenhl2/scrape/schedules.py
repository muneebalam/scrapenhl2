"""
This module contains methods related to season schedules.
"""

import datetime
import functools
import os.path

import feather
import pandas as pd

import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.team_info as team_info


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


def get_season_schedule_filename(season):
    """
    Gets the filename for the season's schedule file
    :param season: int, the season
    :return: /scrape/data/other/[season]_schedule.feather
    """
    return os.path.join(organization.get_other_data_folder(), '{0:d}_schedule.feather'.format(season))


def get_season_schedule(season):
    """
    Gets the the season's schedule file from memory.
    :param season: int, the season
    :return: file (originally from /scrape/data/other/[season]_schedule.feather)
    """
    return _SCHEDULES[season]


def _get_season_schedule(season):
    """
    Gets the the season's schedule file. Stored as a feather file for fast read/write
    :param season: int, the season
    :return: file from /scrape/data/other/[season]_schedule.feather
    """
    return feather.read_dataframe(get_season_schedule_filename(season))


def write_season_schedule(df, season, force_overwrite):
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
    :param season: int
        the season
    :return: str
        https://statsapi.web.nhl.com/api/v1/schedule?startDate=[season]-09-01&endDate=[season+1]-06-25
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
    _SCHEDULES = {season: _get_season_schedule(season) for season in range(2005, _CURRENT_SEASON + 1)}


_CURRENT_SEASON = None
_SCHEDULES = None
schedule_setup()
