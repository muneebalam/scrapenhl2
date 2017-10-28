"""
This module contains method related to team logs.
"""

import os.path

import feather
import pandas as pd

import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.team_info as team_info


def get_team_pbp(season, team):
    """
    Returns the pbp of given team in given season across all games.
    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return: df, the pbp of given team in given season
    """
    return feather.read_dataframe(get_team_pbp_filename(season, team_info.team_as_str(team, True)))


def get_team_toi(season, team):
    """
    Returns the toi of given team in given season across all games.
    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return: df, the toi of given team in given season
    """
    return feather.read_dataframe(get_team_toi_filename(season, team_info.team_as_str(team, True)))


def write_team_pbp(pbp, season, team):
    """
    Writes the given pbp dataframe to file.
    :param pbp: df, the pbp of given team in given season
    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return: nothing
    """
    if pbp is None:
        print('PBP df is None, will not write team log')
        return
    feather.write_dataframe(pbp, get_team_pbp_filename(season, team_info.team_as_str(team, True)))


def write_team_toi(toi, season, team):
    """

    :param toi: df, team toi for this season
    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    if toi is None:
        print('TOI df is None, will not write team log')
        return
    try:
        feather.write_dataframe(toi, get_team_toi_filename(season, team_info.team_as_str(team, True)))
    except ValueError:
        # Need dtypes to be numbers or strings. Sometimes get objs instead
        for col in toi:
            try:
                toi.loc[:, col] = pd.to_numeric(toi[col])
            except ValueError:
                toi.loc[:, col] = toi[col].astype(str)
        feather.write_dataframe(toi, get_team_toi_filename(season, team_info.team_as_str(team, True)))


def get_team_pbp_filename(season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    return os.path.join(organization.get_season_team_pbp_folder(season),
                        "{0:s}.feather".format(team_info.team_as_str(team, abbreviation=True)))


def get_team_toi_filename(season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    return os.path.join(organization.get_season_team_toi_folder(season),
                        "{0:s}.feather".format(team_info.team_as_str(team, abbreviation=True)))
