"""
This module contains methods related to scraping games.
"""
import os.path

import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.team_info as team_info


def most_recent_game_id(team1, team2):
    """
    A convenience function to get the most recent game (this season) between two teams.

    :param team1: str, a team
    :param team2: str, a team

    :return: int, a game number
    """
    return find_recent_games(team1, team2).Game.iloc[0]


def find_recent_games(team1, team2=None, limit=1):
    """
    A convenience function that lists the most recent in progress or final games for specified team(s)

    :param team1: str, a team
    :param team2: str, a team (optional)
    :param limit: How many games to return

    :return: df with relevant rows
    """
    sch = schedules.get_season_schedule(schedules.get_current_season())
    sch = sch[sch.Status != "Scheduled"]

    t1 = team_info.team_as_id(team1)
    sch = sch[(sch.Home == t1) | (sch.Road == t1)]
    if team2 is not None:
        t2 = team_info.team_as_id(team2)
        sch = sch[(sch.Home == t2) | (sch.Road == t2)]

    return sch.sort_values('Game', ascending=False).iloc[:limit, :]


def get_player_5v5_log_filename(season):
    """
    Gets the filename for the season's player log file. Includes 5v5 CF, CA, TOI, and more.

    :param season: int, the season

    :return: str, /scrape/data/other/[season]_player_log.feather
    """
    return os.path.join(organization.get_other_data_folder(), '{0:d}_player_5v5_log.feather'.format(season))

