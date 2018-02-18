"""
This module contains method related to team logs.
"""

import os.path

import pandas as pd
from tqdm import tqdm
import sqlite3

from scrapenhl2.scrape import organization, parse_pbp, parse_toi, schedules, team_info, general_helpers as helpers, \
    scrape_toi, manipulate_schedules


def _get_team_pbp_log_colnames_coltypes():
    """
    Returns schedule column names and types (hard coded)

    - Season INT PRIMARY KEY
    - Game INT PRIMARY KEY
    - Index INT PRIMARY KEY
    - Period INT
    - MinSec CHAR
    - Event CHAR
    - Team INT
    - Actor CHAR
    - ActorRole CHAR
    - Recipient CHAR
    - RecipientRole CHAR
    - FocusTeam INT
    - Home INT
    - Road INT
    - Note CHAR
    - TeamScore INT
    - TeamStrength CHAR
    - OppScore INT
    - OppStrength CHAR
    - Time INT
    - X INT
    - Y INT

    :return: list
    """

    return [('Season', 'INT'), ('Game', 'INT'), ('EventIndex', 'INT'),
            ('Period', 'INT'), ('MinSec', 'CHAR'), ('Event', 'CHAR'), ('Team', 'INT'),
            ('Actor', 'CHAR'), ('ActorRole', 'CHAR'), ('Recipient', 'CHAR'), ('RecipientRole', 'CHAR',),
            ('FocusTeam', 'INT'), ('Home', 'INT'), ('Road', 'INT'), ('Note', 'CHAR'),
            ('TeamScore', 'INT'), ('OppScore', 'INT'), ('TeamStrength', 'CHAR'), ('OppStrength', 'CHAR'),
            ('Time', 'INT'), ('X', 'INT'), ('Y', 'INT')]


def _create_team_pbp_table(team):
    """
    Creates team pbp table

    :param team: int or str
    :return:
    """
    team = team_info.team_as_id(team)
    cols = _get_team_pbp_log_colnames_coltypes()
    cols = ',\n'.join([' '.join(row) for row in cols])
    query = 'CREATE TABLE Pbp (\n{0:s},\nPRIMARY KEY (Season, Game, EventIndex))'.format(cols)
    _TL_CURSORS[team].execute(query)
    _TL_CONNS[team].commit()


def get_team_pbp(season, team):
    """
    Gets team pbp.

    :param season: int
    :param team: int or str
    :return:
    """
    return pd.read_sql_query('SELECT * FROM Pbp WHERE Season = {0:d}'.format(season),
                             _TL_CONNS[team_info.team_as_id(team)])


def _get_team_toi_log_colnames_coltypes():
    """
    Returns schedule column names and types (hard coded)

    - Season INT PRIMARY KEY
    - Game INT PRIMARY KEY
    - Time INT PRIMARY KEY
    - Home INT
    - Road INT
    - Team1 CHAR
    - Team2 CHAR
    - Team3 CHAR
    - Team4 CHAR
    - Team5 CHAR
    - Team6 CHAR
    - TeamG CHAR
    - Opp1 CHAR
    - Opp2 CHAR
    - Opp3 CHAR
    - Opp4 CHAR
    - Opp5 CHAR
    - Opp6 CHAR
    - OppG CHAR
    - TeamScore INT
    - TeamStrength CHAR
    - OppScore INT
    - OppStrength CHAR


    :return: list
    """

    return [('Season', 'INT'), ('Game', 'INT'), ('Time', 'INT'), ('Home', 'INT'), ('Road', 'OMT'),
            ('Team1', 'CHAR'), ('Team2', 'CHAR'), ('Team3', 'CHAR'), ('Team4', 'CHAR'), ('Team5', 'CHAR'),
            ('Team6', 'CHAR'), ('TeamG', 'CHAR'), ('TeamScore', 'CHAR'), ('TeamStrength', 'CHAR'),
            ('Opp1', 'CHAR'), ('Opp2', 'CHAR'), ('Opp3', 'CHAR'), ('Opp4', 'CHAR'), ('Opp5', 'CHAR'),
            ('Opp6', 'CHAR'), ('OppG', 'CHAR'), ('OppScore', 'INT'), ('OppStrength', 'CHAR')]


def _create_team_toi_table(team):
    """
    Creates team pbp table

    :param team: int or str
    :return:
    """
    team = team_info.team_as_id(team)
    cols = _get_team_toi_log_colnames_coltypes()
    cols = ',\n'.join([' '.join(row) for row in cols])
    query = 'CREATE TABLE Toi (\n{0:s},\nPRIMARY KEY ({1:s}, {2:s}, {3:s}))'.format(cols, 'Season', 'Game', 'Time')
    _TL_CURSORS[team].execute(query)
    _TL_CONNS[team].commit()


def get_team_toi(season, team):
    """
    Gets team toi.

    :param season: int
    :param team: int or str
    :return:
    """
    return pd.read_sql_query('SELECT * FROM Toi WHERE Season = {0:d}'.format(season),
                             _TL_CONNS[team_info.team_as_id(team)])


def update_team_logs(season, games, from_scratch=False):
    """
    This method looks at the schedule for the given season and writes pbp for scraped games to file.
    It also adds the strength at each pbp event to the log. It only includes games that have both PBP *and* TOI.
    If games provided are already included in team logs, will overwrite ("force_overwrite" is always True in that sense).

    :param season: int, the season
    :param games, list of int.
    :param from_scratch: bool, whether to generate from scratch

    :return: nothing
    """
    # Create team dbs if need be
    allteams = {schedules.get_home_team(season, game) for game in games} \
        .union({schedules.get_road_team(season, game) for game in games})
    for team in allteams:
        try:
            _ = get_team_pbp(season, team)
        except pd.io.sql.DatabaseError:
            _create_team_pbp_table(team)
        try:
            _ = get_team_toi(season, team)
        except pd.io.sql.DatabaseError:
            _create_team_toi_table(team)

    if from_scratch:
        for team in allteams:
            _TL_CURSORS[team].execute('DELETE FROM Pbp WHERE Season = {0:d}'.format(season))
            _TL_CURSORS[team].execute('DELETE FROM Toi WHERE Season = {0:d}'.format(season))

    for game in tqdm(games, desc = 'Adding games to team logs'):
        hteam = schedules.get_home_team(season, game)
        rteam = schedules.get_road_team(season, game)

        pbp = parse_pbp.get_parsed_pbp(season, game).rename(columns={'Index': 'EventIndex'})
        toi = parse_toi.get_parsed_toi(season, game)

        # Want both pbp and toi to exist
        if pbp is None or toi is None or len(pbp) == 0 or len(toi) == 0:
            continue

        # Going to be joining on time, make sure both are type int
        pbp.loc[:, 'Time'] = pbp.Time.astype(int)
        toi.loc[:, 'Time'] = toi.Time.astype(int)

        # Add strength to PBP
        pbp = pbp.merge(toi[['Time', 'HomeStrength', 'RoadStrength']], how='left', on='Time')
        # Add scores to TOI
        toi = toi.merge(pbp[['Time', 'HomeScore', 'RoadScore']], how='left', on='Time')
        # Forward fill score
        toi = toi.assign(HomeScore = toi.HomeScore.fillna(method='ffill'),
                         RoadScore = toi.RoadScore.fillna(method='ffill'))
        # Time zero was 5v5
        pbp.loc[pbp.Time == 0, 'HomeStrength':'RoadStrength'] = '5'

        # Add game
        pbp = pbp.assign(Game = game)
        toi = toi.assign(Game = game)

        # Rename columns for home team and road team
        hpbp = pbp.rename(columns={x: x.replace('Home', 'Team').replace('Road', 'Opp') for x in pbp.columns})
        rpbp = pbp.rename(columns={x: x.replace('Home', 'Opp').replace('Road', 'Team') for x in pbp.columns})
        htoi = toi.rename(columns={x: x.replace('Home', 'Team').replace('Road', 'Opp') for x in toi.columns})
        rtoi = toi.rename(columns={x: x.replace('Home', 'Opp').replace('Road', 'Team') for x in toi.columns})

        # Some cols have just H and R
        toicols = list(toi.columns)
        htoi = htoi.rename(columns={x: x.replace('H', 'Team').replace('R', 'Opp') for x in
                                   toicols[toicols.index('H1'):toicols.index('RG') + 1]})
        rtoi = rtoi.rename(columns={x: x.replace('H', 'Opp').replace('R', 'Team') for x in
                                   toicols[toicols.index('H1'):toicols.index('RG') + 1]})

        # Add home and road (would have gotten renamed above)
        hpbp = hpbp.assign(Home = hteam, Road = rteam)
        htoi = htoi.assign(Home = hteam, Road = rteam)
        rpbp = rpbp.assign(Home = hteam, Road = rteam)
        rtoi = rtoi.assign(Home = hteam, Road = rteam)

        hpbp.to_sql('Pbp', _TL_CONNS[hteam], if_exists = 'append', index = False)
        htoi.to_sql('Toi', _TL_CONNS[hteam], if_exists = 'append', index = False)
        rpbp.to_sql('Pbp', _TL_CONNS[rteam], if_exists = 'append', index = False)
        rtoi.to_sql('Toi', _TL_CONNS[rteam], if_exists = 'append', index = False)

    for team in allteams:
        _TL_CONNS[team].commit()


def update_pbplog(team, **kwargs):
    """

    :param team:
    :param kwargs:
    :return:
    """
    helpers.replace_into_table(_TL_CURSORS[team_info.team_as_id(team)], 'Pbp', **kwargs)


def update_toilog(team, **kwargs):
    """

    :param team:
    :param kwargs:
    :return:
    """
    helpers.replace_into_table(_TL_CURSORS[team_info.team_as_id(team)], 'Toi', **kwargs)


def get_team_log_filename(team):
    """
    Returns db filename

    :param team: int or str
    :return:
    """
    return os.path.join(organization.get_team_data_folder(), '{0:s}{1:d}.sqlite'.format(
        team_info.team_as_str(team), team_info.team_as_id(team))) # Use both because some duplicate abbrevs


def get_team_log_connection(team):
    """
    Gets connections to team pbplog db.

    :param team: str or int
    :return: dict
    """
    return sqlite3.connect(get_team_log_filename(team))


def team_setup():
    """
    Creates team log-related folders.

    :return: nothing
    """
    for season in range(2005, schedules.get_current_season() + 1):
        organization.check_create_folder(organization.get_season_team_pbp_folder(season))
    for season in range(2005, schedules.get_current_season() + 1):
        organization.check_create_folder(organization.get_season_team_toi_folder(season))

    allteams = {x[0] for x in schedules._SCH_CURSOR.execute('SELECT DISTINCT Home FROM Schedule').fetchall()} \
        .union({x[0] for x in schedules._SCH_CURSOR.execute('SELECT DISTINCT Road FROM Schedule').fetchall()})
    global _TL_CONNS, _TL_CURSORS
    _TL_CONNS = {team: get_team_log_connection(team) for team in allteams}
    _TL_CURSORS = {team: conn.cursor() for team, conn in _TL_CONNS.items()}

_TL_CONNS = {}
_TL_CURSORS = {}
team_setup()
