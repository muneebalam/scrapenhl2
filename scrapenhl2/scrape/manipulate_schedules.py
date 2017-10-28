"""
This module contains methods related to generating and manipulating schedules.
"""

import json
import os.path
import urllib.error
import urllib.request

import pandas as pd

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

    # Edit relevant schedule files
    df = schedules.get_season_schedule(season)
    df.loc[df.Game == game, 'Result'] = result

    # Write to file and refresh schedule in memory
    schedules.write_season_schedule(df, season, True)


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
    df = schedules.get_season_schedule(season)
    df.loc[df.Game == game, 'HomeCoach'] = homecoach
    df.loc[df.Game == game, 'RoadCoach'] = roadcoach

    # Write to file and refresh schedule in memory
    schedules.write_season_schedule(df, season, True)


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

    :param df: dataframe
        season schedule dataframe as created by _create_schedule_dataframe_from_json
    :param season: int
        the season
    :return: df, with coaches, result, and status filled in
    """

    if os.path.exists(schedules.get_season_schedule_filename(season)):
        # only final games--this way pbp status and toistatus will be ok.
        cur_season = schedules.get_season_schedule(season).query('Status == "Final"')
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
    url = schedules.get_season_schedule_url(season)
    with urllib.request.urlopen(url) as reader:
        page = reader.read()

    page2 = json.loads(page)
    df = _create_schedule_dataframe_from_json(page2)
    df.loc[:, 'Season'] = season

    # Last step: we fill in some info from the pbp. If current schedule already exists, fill in that info.
    df = _fill_in_schedule_from_pbp(df, season)
    schedules.write_season_schedule(df, season, force_overwrite)


def update_schedule_with_pbp_scrape(season, game):
    """
    Updates the schedule file saying that specified game's pbp has been scraped.
    :param season: int, the season
    :param game: int, the game, or list of ints
    :return: updated schedule
    """
    df = schedules.get_season_schedule(season)
    if helpers.check_types(game):
        df.loc[df.Game == game, "PBPStatus"] = "Scraped"
    else:
        df.loc[df.Game.isin(game), "PBPStatus"] = "Scraped"
    schedules.write_season_schedule(df, season, True)
    return schedules.get_season_schedule(season)


def update_schedule_with_toi_scrape(season, game):
    """
    Updates the schedule file saying that specified game's toi has been scraped.
    :param season: int, the season
    :param game: int, the game, or list of int
    :return: nothing
    """
    df = schedules.get_season_schedule(season)
    if helpers.check_types(game):
        df.loc[df.Game == game, "TOIStatus"] = "Scraped"
    else:
        df.loc[df.Game.isin(game), "TOIStatus"] = "Scraped"
    schedules.write_season_schedule(df, season, True)
    return schedules.get_season_schedule(season)
