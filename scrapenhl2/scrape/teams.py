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
    - Actor INT
    - ActorRole CHAR
    - Recipient INT
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
            ('Actor', 'INT'), ('ActorRole', 'CHAR'), ('Recipient', 'INT'), ('RecipientRole', 'CHAR',),
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
    - Team1 INT
    - Team2 INT
    - Team3 INT
    - Team4 INT
    - Team5 INT
    - Team6 INT
    - TeamG INT
    - Opp1 INT
    - Opp2 INT
    - Opp3 INT
    - Opp4 INT
    - Opp5 INT
    - Opp6 INT
    - OppG INT
    - TeamScore INT
    - TeamStrength CHAR
    - OppScore INT
    - OppStrength CHAR


    :return: list
    """

    return [('Season', 'INT'), ('Game', 'INT'), ('Time', 'INT'), ('Home', 'INT'), ('Road', 'OMT'),
            ('Team1', 'INT'), ('Team2', 'INT'), ('Team3', 'INT'), ('Team4', 'INT'), ('Team5', 'INT'),
            ('Team6', 'INT'), ('TeamG', 'INT'), ('TeamScore', 'INT'), ('TeamStrength', 'CHAR'),
            ('Opp1', 'INT'), ('Opp2', 'INT'), ('Opp3', 'INT'), ('Opp4', 'INT'), ('Opp5', 'INT'),
            ('Opp6', 'INT'), ('OppG', 'INT'), ('OppScore', 'INT'), ('OppStrength', 'CHAR')]


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

        pbp = parse_pbp.get_parsed_pbp(season, game)
        toi = parse_toi.get_parsed_toi(season, game)

        # Want both pbp and toi to exist
        if pbp is None or toi is None or len(pbp) == 0 or len(toi) == 0:
            continue

        # Add scores to TOI
        pbp = pbp.merge(toi[['Time', 'HomeStrength', 'RoadStrength']], how='left', on='Time')
        # Add strength to PBP
        toi = toi.merge(pbp[['Time', 'HomeScore', 'RoadScore']], how='left', on='Time')
        # Forward fill score
        toi = toi.assign(HomeScore = toi.HomeScore.fillna(method='ffill'),
                         RoadScore = toi.RoadScore.fillna(method='ffill'))

        # Add game
        pbp = pbp.assign(Game = game)
        toi = toi.assign(Game = game)

        # Rename columns for home team and road team
        hpbp = pbp.rename(columns={x.replace('Home', 'Team').replace('Road', 'Opp') for x in pbp.columns})
        htoi = toi.rename(columns={x.replace('Home', 'Team').replace('Road', 'Opp') for x in toi.columns})
        rpbp = pbp.rename(columns={x.replace('Home', 'Opp').replace('Road', 'Team') for x in pbp.columns})
        rtoi = toi.rename(columns={x.replace('Home', 'Opp').replace('Road', 'Team') for x in toi.columns})

        # Add home and road (would have gotten renamed above)
        hpbp = hpbp.assign(Home = hteam, Road = rteam)
        htoi = htoi.assign(Home = hteam, Road = rteam)
        rpbp = rpbp.assign(Home = hteam, Road = rteam)
        rtoi = rtoi.assign(Home = hteam, Road = rteam)

        hpbp.to_sql('Pbp', _TL_CONNS[hteam], if_exists = 'append', index = False)
        #htoi.to_sql('Toi', _TL_CONNS[hteam], if_exists = 'append', index = False)
        #rpbp.to_sql('Pbp', _TL_CONNS[rteam], if_exists = 'append', index = False)
        #rtoi.to_sql('Toi', _TL_CONNS[rteam], if_exists = 'append', index = False)


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


def update_team_logs_backup():

    for team in tqdm(allteams, desc = 'Updating team logs'):
        #print('Updating team log for {0:d} {1:s}'.format(season, team_info.team_as_str(team)))

        # Compare existing log to schedule to find missing games
        newgames = new_games_to_do[(new_games_to_do.Home == team) | (new_games_to_do.Road == team)]
        if force_overwrite:
            pbpdf = None
            toidf = None
        else:
            # Read currently existing ones for each team and anti join to schedule to find missing games
            try:
                pbpdf = get_team_pbp(season, team)
                if force_games is not None:
                    pbpdf = helpers.anti_join(pbpdf, pd.DataFrame({'Game': list(force_games)}), on='Game')
                newgames = newgames.merge(pbpdf[['Game']].drop_duplicates(), how='outer', on='Game', indicator=True)
                newgames = newgames[newgames._merge == "left_only"].drop('_merge', axis=1)
            except OSError:
                pbpdf = None
            except OSError:  # pyarrow (feather) FileNotFoundError equivalent
                pbpdf = None

            try:
                toidf = get_team_toi(season, team)
                if force_games is not None:
                    toidf = helpers.anti_join(toidf, pd.DataFrame({'Game': list(force_games)}), on='Game')
            except OSError:
                toidf = None
            except OSError:  # pyarrow (feather) FileNotFoundError equivalent
                toidf = None

        for i, gamerow in newgames.iterrows():
            game = gamerow[1]
            home = gamerow[2]
            road = gamerow[4]

            # load parsed pbp and toi
            try:
                try:
                    gamepbp = None
                    gamepbp = parse_pbp.get_parsed_pbp(season, game)
                except OSError:
                    print("Check PBP for", season, game)
                try:
                    gametoi = None
                    gametoi = parse_toi.get_parsed_toi(season, game)
                except OSError:
                    # try html
                    scrape_toi.scrape_game_toi_from_html(season, game)
                    parse_toi.parse_game_toi_from_html(season, game)
                    manipulate_schedules.update_schedule_with_toi_scrape(season, game)
                    try:
                        gametoi = parse_toi.get_parsed_toi(season, game)
                    except OSError:
                        print('Check TOI for', season, game)

                if gamepbp is not None and gametoi is not None and len(gamepbp) > 0 and len(gametoi) > 0:
                    # Rename score and strength columns from home/road to team/opp
                    if team == home:
                        gametoi = gametoi.assign(TeamStrength=gametoi.HomeStrength, OppStrength=gametoi.RoadStrength) \
                            .drop({'HomeStrength', 'RoadStrength'}, axis=1)
                        gamepbp = gamepbp.assign(TeamScore=gamepbp.HomeScore, OppScore=gamepbp.RoadScore) \
                            .drop({'HomeScore', 'RoadScore'}, axis=1)
                    else:
                        gametoi = gametoi.assign(TeamStrength=gametoi.RoadStrength, OppStrength=gametoi.HomeStrength) \
                            .drop({'HomeStrength', 'RoadStrength'}, axis=1)
                        gamepbp = gamepbp.assign(TeamScore=gamepbp.RoadScore, OppScore=gamepbp.HomeScore) \
                            .drop({'HomeScore', 'RoadScore'}, axis=1)

                    # add scores to toi and strengths to pbp
                    gamepbp = gamepbp.merge(gametoi[['Time', 'TeamStrength', 'OppStrength']], how='left', on='Time')
                    gametoi = gametoi.merge(gamepbp[['Time', 'TeamScore', 'OppScore']], how='left', on='Time')
                    gametoi.loc[:, 'TeamScore'] = gametoi.TeamScore.fillna(method='ffill')
                    gametoi.loc[:, 'OppScore'] = gametoi.OppScore.fillna(method='ffill')

                    # Switch TOI column labeling from H1/R1 to Team1/Opp1 as appropriate
                    cols_to_change = list(gametoi.columns)
                    cols_to_change = [x for x in cols_to_change if len(x) == 2]  # e.g. H1
                    if team == home:
                        swapping_dict = {'H': 'Team', 'R': 'Opp'}
                        colchanges = {c: swapping_dict[c[0]] + c[1] for c in cols_to_change}
                    else:
                        swapping_dict = {'H': 'Opp', 'R': 'Team'}
                        colchanges = {c: swapping_dict[c[0]] + c[1] for c in cols_to_change}
                    gametoi = gametoi.rename(columns=colchanges)

                    # finally, add game, home, and road to both dfs
                    gamepbp.loc[:, 'Game'] = game
                    gamepbp.loc[:, 'Home'] = home
                    gamepbp.loc[:, 'Road'] = road
                    gametoi.loc[:, 'Game'] = game
                    gametoi.loc[:, 'Home'] = home
                    gametoi.loc[:, 'Road'] = road

                    # concat toi and pbp
                    if pbpdf is None:
                        pbpdf = gamepbp
                    else:
                        pbpdf = pd.concat([pbpdf, gamepbp])
                    if toidf is None:
                        toidf = gametoi
                    else:
                        toidf = pd.concat([toidf, gametoi])

            except FileNotFoundError:
                pass

        # write to file
        if pbpdf is not None:
            pbpdf.loc[:, 'FocusTeam'] = team
        if toidf is not None:
            toidf.loc[:, 'FocusTeam'] = team

        write_team_pbp(pbpdf, season, team)
        write_team_toi(toidf, season, team)
        #print('Done with team logs for {0:d} {1:s} ({2:d}/{3:d})'.format(
        #    season, team_info.team_as_str(team), teami + 1, len(allteams)))


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
