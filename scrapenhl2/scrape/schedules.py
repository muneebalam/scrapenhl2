"""
This module contains methods related to season schedules.
"""

import datetime
import os.path

import feather
import pandas as pd

import scrapenhl2.scrape.organization as organization


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
    return feather.read_dataframe(organization.get_filenames.get_season_schedule_filename(season))


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
    schedule_setup()


def schedule_setup():
    """
    Reads current season and schedules into memory.
    :return: nothing
    """
    global _SCHEDULES, _CURRENT_SEASON
    _CURRENT_SEASON = _get_current_season()
    _SCHEDULES = {season: _get_season_schedule(season) for season in range(2005, _CURRENT_SEASON + 1)}


schedule_setup()
