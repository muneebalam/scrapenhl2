"""
This module contains all methods for accessing and writing files.
"""

import datetime

import feather

import scrapenhl2.scrape.get_filenames as get_filenames


def get_team_pbp(season, team):
    """
    Returns the pbp of given team in given season across all games.
    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return: df, the pbp of given team in given season
    """
    return feather.read_dataframe(get_filenames.get_team_pbp_filename(season, team_as_str(team, True)))


def get_team_toi(season, team):
    """
    Returns the toi of given team in given season across all games.
    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return: df, the toi of given team in given season
    """
    return feather.read_dataframe(get_filenames.get_team_toi_filename(season, team_as_str(team, True)))


def write_team_pbp(pbp, season, team):
    """
    Writes the given pbp dataframe to file.
    :param pbp: df, the pbp of given team in given season
    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return: nothing
    """
    if pbp is None:
        ed.print_and_log('PBP df is None, will not write team log', 'warn')
        return
    feather.write_dataframe(pbp, get_filenames.get_team_pbp_filename(season, team_as_str(team, True)))


def write_team_toi(toi, season, team):
    """

    :param toi: df, team toi for this season
    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    if toi is None:
        ed.print_and_log('TOI df is None, will not write team log', 'warn')
        return
    try:
        feather.write_dataframe(toi, get_filenames.get_team_toi_filename(season, team_as_str(team, True)))
    except ValueError:
        # Need dtypes to be numbers or strings. Sometimes get objs instead
        for col in toi:
            try:
                toi.loc[:, col] = pd.to_numeric(toi[col])
            except ValueError:
                toi.loc[:, col] = toi[col].astype(str)
        feather.write_dataframe(toi, get_filenames.get_team_toi_filename(season, team_as_str(team, True)))


def _get_team_info_file():
    """
    Returns the team information file. This is stored as a feather file for fast read/write.
    :return: file from /scrape/data/other/TEAM_INFO.feather
    """
    return feather.read_dataframe(get_filenames.get_team_info_filename())


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
    feather.write_dataframe(df, get_filenames.get_team_info_filename())


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
    return feather.read_dataframe(get_filenames.get_player_log_filename())


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
    return feather.read_dataframe(get_filenames.get_player_ids_filename())


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
    return feather.read_dataframe(get_filenames.get_season_schedule_filename(season))


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


def write_player_log_file(df):
    """
    Writes the given dataframe to file as the player log filename
    :param df: pandas dataframe
    :return: nothing
    """
    feather.write_dataframe(df.drop_duplicates(), get_filenames.get_player_log_filename())





def setup():
    """
    Loads team info file into memory.
    :return: nothing
    """
    global _TEAMS, _PLAYERS, _PLAYER_LOG, _SCHEDULES, _CURRENT_SEASON

    _CURRENT_SEASON = _get_current_season()
    _TEAMS = _get_team_info_file()
    _PLAYERS = _get_player_ids_file()
    _PLAYER_LOG = _get_player_log_file()
    _SCHEDULES = {season: _get_season_schedule(season) for season in range(2005, _CURRENT_SEASON + 1)}
