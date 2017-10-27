"""
This module contains all methods for accessing and writing files.
"""

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


def setup():
    """
    Loads team info file into memory.
    :return: nothing
    """
    global _TEAMS
    _TEAMS = _get_team_info_file()
