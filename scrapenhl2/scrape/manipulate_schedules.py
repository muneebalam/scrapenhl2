"""
This module contains methods related to generating and manipulating schedules.
"""

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.schedules as schedules


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

    schedules.update_schedule(update_or_replace='update', Season=season, Game=game, Result=result)


def _update_schedule_with_coaches(season, game, homecoach, roadcoach):
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
    schedules.update_schedule(update_or_replace='update', Season=season, Game=game,
                              HomeCoach=homecoach, RoadCoach=roadcoach)


def update_schedule_with_pbp_scrape(season, game):
    """
    Updates the schedule file saying that specified game's pbp has been scraped.

    :param season: int, the season
    :param game: int, the game, or list of ints

    :return:
    """
    if not isinstance(game, list) and not isinstance(game, set):
        game = [game]
    for g in game:
        schedules.update_schedule(update_or_replace='update', Season=season, Game=g, PBPStatus='Scraped')


def update_schedule_with_toi_scrape(season, game):
    """
    Updates the schedule file saying that specified game's toi has been scraped.

    :param season: int, the season
    :param game: int, the game, or list of int

    :return: nothing
    """
    if not isinstance(game, list) and not isinstance(game, set):
        game = [game]
    for g in game:
        schedules.update_schedule(update_or_replace='update', Season=season, Game=g, TOIStatus='Scraped')


def update_schedule_with_result_using_pbp(pbp, season, game):
    """
    Uses the PbP to update results for this game.

    :param pbp: json, the pbp for this game
    :param season: int, the season
    :param game: int, the game

    :return: nothing
    """

    gameinfo = schedules.get_game_data_from_schedule(season, game)
    result = None  # In case they have the same score. Like 2006 10009 has incomplete data, shows 0-0

    # If game is not final yet, don't do anything
    if gameinfo['Status'] != 'Final':
        return False

    # If one team one by at least two, we know it was a regulation win
    if gameinfo['HomeScore'] >= gameinfo['RoadScore'] + 2:
        result = 'W'
    elif gameinfo['RoadScore'] >= gameinfo['HomeScore'] + 2:
        result = 'L'
    else:
        # Check for the final period
        finalplayperiod = helpers.try_to_access_dict(pbp, 'liveData', 'linescore', 'currentPeriodOrdinal')

        # Identify SO vs OT vs regulation
        if finalplayperiod is None:
            pass
        elif finalplayperiod == 'SO':
            if gameinfo['HomeScore'] > gameinfo['RoadScore']:
                result = 'SOW'
            elif gameinfo['RoadScore'] > gameinfo['HomeScore']:
                result = 'SOL'
        elif finalplayperiod[-2:] == 'OT':
            if gameinfo['HomeScore'] > gameinfo['RoadScore']:
                result = 'OTW'
            elif gameinfo['RoadScore'] > gameinfo['HomeScore']:
                result = 'OTL'
        else:
            if gameinfo['HomeScore'] > gameinfo['RoadScore']:
                result = 'W'
            elif gameinfo['RoadScore'] > gameinfo['HomeScore']:
                result = 'L'

    update_schedule_with_result(season, game, result)


def update_schedule_with_coaches(pbp, season, game):
    """
    Uses the PbP to update coach info for this game.

    :param pbp: json, the pbp for this game
    :param season: int, the season
    :param game: int, the game

    :return: nothing
    """

    homecoach = helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'coaches', 0, 'person',
                                           'fullName')
    roadcoach = helpers.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'coaches', 0, 'person',
                                           'fullName')
    _update_schedule_with_coaches(season, game, homecoach, roadcoach)
