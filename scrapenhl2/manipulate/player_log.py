"""
This module contains methods related to generating the 5v5 player log.
"""

import os
from tqdm import tqdm
import sqlite3

from scrapenhl2.scrape import schedules, teams, organization, players


def create_toi_table():
    """Creates table Toi with Season, Game, PlayerID, Secs"""
    cols = ',\n'.join(['Season INT', 'Game INT', 'PlayerID CHAR', 'Secs INT'])
    query = 'CREATE TABLE Toi (\n{0:s},\nPRIMARY KEY ({1:s}, {2:s}, {3:s}))'.format(cols, 'Season', 'Game', 'PlayerID')
    _PL_CURSOR.execute(query)
    _PL_CONN.commit()


def create_scoring_table():
    """Creates table Scoring with Season, Game, PlayerID, G, A1, A2"""
    cols = ',\n'.join(['Season INT', 'Game INT', 'PlayerID CHAR', 'G INT', 'A1 INT', 'A2 INT'])
    query = 'CREATE TABLE Scoring (\n{0:s},\nPRIMARY KEY ({1:s}, {2:s}, {3:s}))'.format(cols, 'Season', 'Game',
                                                                                        'PlayerID')
    _PL_CURSOR.execute(query)
    _PL_CONN.commit()


def get_playerlog_filename():
    """
    Returns other data folder / playerlog.sqlite

    :return:
    """
    return players.get_player_log_filename()


def get_playerlog_connection():
    """
    Returns connection for player log

    :return:
    """
    return sqlite3.connect(get_playerlog_filename())


def generate_player_toion_toioff(season):
    """
    Generates TOION and TOIOFF at 5v5 for each player in this season.
    :param season: int, the season
    :return: df with columns Player, TOION, TOIOFF, and TOI60.
    """

    team_by_team = []
    allteams = schedules.get_teams_in_season(season)
    for team in tqdm(allteams, desc="Parsing Games"):
        if os.path.exists(teams.get_team_toi_filename(season, team)):
            print('Generating TOI60 for {0:d} {1:s} ({2:d}/{3:d})'.format(
                season, team_info.team_as_str(team), i + 1, len(allteams)))
            toi_indiv = get_5v5_player_season_toi(season, team)
            team_by_team.append(toi_indiv)

    toi60 = pd.concat(team_by_team)
    toi60 = toi60.groupby('PlayerID').sum().reset_index()
    toi60.loc[:, 'TOI%'] = toi60.TOION / (toi60.TOION + toi60.TOIOFF)
    toi60.loc[:, 'TOI60'] = toi60['TOI%'] * 60

    return toi60

_PL_CONN = get_playerlog_connection()
_PL_CURSOR = _PL_CONN.cursor()
