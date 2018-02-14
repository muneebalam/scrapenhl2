"""
This module contains methods related to scraping games.
"""
import os.path
import re
import datetime

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


def find_recent_games(team1, team2=None, limit=1, season=None):
    """
    A convenience function that lists the most recent in progress or final games for specified team(s)

    :param team1: str, a team
    :param team2: str, a team (optional)
    :param limit: How many games to return
    :param season: int, the season

    :return: df with relevant rows
    """
    if season is None:
        season = schedules.get_current_season()
    sch = schedules.get_season_schedule(season)
    #sch = sch[sch.Status != "Scheduled"]  # doesn't work if data hasn't been updated
    sch = sch[sch.Date <= datetime.datetime.now().strftime('%Y-%m-%d')]

    t1 = team_info.team_as_id(team1)
    sch = sch[(sch.Home == t1) | (sch.Road == t1)]
    if team2 is not None:
        t2 = team_info.team_as_id(team2)
        sch = sch[(sch.Home == t2) | (sch.Road == t2)]

    return sch.sort_values('Game', ascending=False).iloc[:limit, :]


def find_playoff_game(searchstr):
    """
    Finds playoff game id based on string specified
    :param searchstr: e.g. WSH PIT 2016 Game 5
    :return: (season, game)
    """

    parts = searchstr.split(' ')
    teams = []
    for part in parts:
        if re.match(r'^[A-z]{3}$', part.strip()):
            teams.append(part.upper())
    if len(teams) != 2:
        return

    team1, team2 = teams[:2]

    searchstr += ' '
    if re.search(r'\s\d{4}\s', searchstr) is not None:
        season = int(re.search(r'\s\d{4}\s', searchstr).group(0))
    else:
        season = schedules.get_current_season()

    # Get game with a 5-digit regex
    if re.search(r'\s\d\s', searchstr) is not None:
        gamenum = int(re.search(r'\s\d\s', searchstr).group(0))
        games = find_recent_games(team1, team2, limit=7, season=season)
        game = games[games.Game % 10 == gamenum].Game.iloc[0]
    else:
        raise ValueError

    return season, game



def get_player_5v5_log_filename(season):
    """
    Gets the filename for the season's player log file. Includes 5v5 CF, CA, TOI, and more.

    :param season: int, the season

    :return: str, /scrape/data/other/[season]_player_log.feather
    """
    return os.path.join(organization.get_other_data_folder(), '{0:d}_player_5v5_log.feather'.format(season))

