import functools
import os
import os.path
import os.path

import feather
import halo
import pandas as pd

import scrapenhl2.scrape.scrape_game as sg
import scrapenhl2.scrape.scrape_setup as ss


def get_player_toion_toioff_filename(season):
    """

    :param season: int, the season
    :return:
    """
    return os.path.join(ss.get_other_data_folder(), '{0:d}_season_toi60.csv'.format(season))


def save_player_toion_toioff_file(df, season):
    """

    :param df:
    :param season: int, the season
    :return:
    """
    df.to_csv(get_player_toion_toioff_filename(season), index=False)


def get_player_toion_toioff_file(season, force_create=False):
    """

    :param season: int, the season
    :param force_create: bool, should this be read from file if possible, or created from scratch
    :return:
    """
    fname = get_player_toion_toioff_filename(season)
    if os.path.exists(fname) and not force_create:
        return pd.read_csv(fname)
    else:
        df = generate_player_toion_toioff(season)
        save_player_toion_toioff_file(df, season)
        return get_player_toion_toioff_file(season)


def get_pbp_events(*args, **kwargs):
    """
    A general method that yields a generator of dataframes of PBP events subject to given limitations.

    Keyword arguments are applied as "or" conditions for each individual keyword (e.g. multiple teams) but as
    "and" conditions otherwise.

    The non-keyword arguments are event types subject to "or" conditions:

    - 'fac' or 'faceoff'
    - 'shot' or 'sog' or 'save'
    - 'hit'
    - 'stop' or 'stoppage'
    - 'block' or 'blocked shot'
    - 'miss' or 'missed shot'
    - 'give' or 'giveaway'
    - 'take' or 'takeaway'
    - 'penl' or 'penalty'
    - 'goal'
    - 'period end'
    - 'period official'
    - 'period ready'
    - 'period start'
    - 'game scheduled'
    - 'gend' or 'game end'
    - 'shootout complete'
    - 'chal' or 'official challenge'
    - 'post', which is not an officially designated event but will be searched for

    Dataframes are returned season-by-season to save on memory. If you want to operate on all seasons,
    process this data before going to the next season.

    Defaults to return all regular-season and playoff events by all teams.

    Supported keyword arguments:

    - add_on_ice: bool. If True, adds on-ice players for each time.
    - players_on_ice: str or int, or list of them, player IDs or names of players on ice for event.
    - players_on_ice_for: like players_on_ice, but players must be on ice for team that "did" event.
    - players_on_ice_ag: like players_on_ice, but players must be on ice for opponent of team that "did" event.
    - team, str or int, or list of them. Teams to filter for.
    - team_for, str or int, or list of them. Team that committed event.
    - team_ag, str or int, or list of them. Team that "received" event.
    - home_team: str or int, or list of them. Home team.
    - road_team: str or int, or list of them. Road team.
    - start_date: str or date, will only return data on or after this date. YYYY-MM-DD
    - end_date: str or date, will only return data on or before this date. YYYY-MM-DD
    - start_season: int, will only return events in or after this season. Defaults to 2010-11.
    - end_season: int, will only return events in or before this season. Defaults to current season.
    - season_type: int or list of int. 1 for preseason, 2 for regular, 3 for playoffs, 4 for ASG, 6 for Oly, 8 for WC.
        Defaults to 2 and 3.
    - start_game: int, start game. Applies only to start season. Game ID will be this, or greater.
    - end_game: int, end game. Applies only to end season. Game ID will be this, or smaller.
    - acting_player: str or int, or list of them, players who committed event (e.g. took a shot).
    - receiving_player: str or int, or list of them, players who received event (e.g. took a hit).
    - strength_hr: tuples or list of them, e.g. (5, 5) or ((5, 5), (4, 4), (3, 3)). This is (Home, Road).
        If neither strength_hr nor strength_to is specified, uses 5v5.
    - strength_to: tuples or list of them, e.g. (5, 5) or ((5, 5), (4, 4), (3, 3)). This is (Team, Opponent).
        If neither strength_hr nor strength_to is specified, uses 5v5.
    - score_diff: int or list of them, acceptable score differences (e.g. 0 for tied, (1, 2, 3) for up by 1-3 goals)
    - start_time: int, seconds elapsed in game. Events returned will be after this.
    - end_time: int, seconds elapsed in game. Events returned will be before this.

    :param args: str, event types to search for (applied "OR", not "AND")
    :param kwargs: keyword arguments specifying filters (applied "AND", not "OR")
    :return: df, a pandas dataframe
    """

    # Read from team logs. Since I store by team, first, read relevant teams' logs
    all_teams_to_read = _teams_to_read(**kwargs)
    all_seasons_to_read = _seasons_to_read(**kwargs)

    for season in all_seasons_to_read:
        df = pd.concat([ss.get_team_pbp(season, team) for team in all_teams_to_read])
        df = _filter_for_team(df, **kwargs)

        df = _filter_for_games(df, **kwargs)

        df = _filter_for_times(df, **kwargs)

        df = _filter_for_strengths(df, **kwargs)

        df = _filter_for_event_types(df, *args)

        # This could take longest, since it involved reading TOI, so leave it until the end
        df = _filter_for_players(df, **kwargs)


def _filter_for_event_types(data, *args):
    """
    Uses
    :param data: a dataframe with pbp data
    :param args: args as given to get_pbp_events, for example
    :return: a dataframe filtered to fit event-related args
    """

    data.loc[:, 'Event2'] = data.Event.str.lower()

    dflst = []
    for arg in args:
        dflst.append(data[data.Event2 == ss.get_event_longname(arg)])
    data = pd.concat(dflst).drop('Event2', axis=1)
    return data


def _filter_for_scores(data, **kwargs):
    """
    Uses the score_diff keyword argument to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit score-related kwargs
    """

    if 'score_diff' in kwargs:
        if isinstance(kwargs['score_diff'], int):
            sds = set((kwargs['score_diff']))
        else:
            sds = set(kwargs['score_diff'])
        data = pd.concat([data[data.TeamScore - data.OppScore == sd] for sd in sds])
    return data


def _filter_for_strengths(data, **kwargs):
    """
    Uses the strength_hr and strength_to keyword arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit strength-related kwargs
    """

    if 'strength_to' in kwargs:
        data = data[(data.TeamStrength == kwargs['strength_to'][0]) & (data.OppStrength == kwargs['strength_to'][1])]

    if 'strength_hr' in kwargs:
        # Find whether team was home or road
        pass

    if 'strength_to' not in kwargs and 'strength_hr' not in kwargs:
        data = data[(data.TeamStrength == 5) & (data.OppStrength == 5)]

    return data


def _filter_for_times(data, **kwargs):
    """
    Uses the start_time and end_time keyword arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit time-related kwargs
    """

    if 'start_time' in kwargs:
        data = data[data.Time >= kwargs['start_time']]
    if 'end_time' in kwargs:
        data = data[data.Time <= kwargs['end_time']]
    return data


def _filter_for_games(data, **kwargs):
    """
    Uses the start_game, end_game, and season_types keyword arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit game number-related kwargs
    """

    if 'start_game' in kwargs:
        startseason = data.Season.min()
        data = data[(data.Season == startseason) & (data.Game >= kwargs['start_game'])]
    if 'end_game' in kwargs:
        endseason = data.Season.max()
        data = data[(data.Season == endseason) & (data.Game <= kwargs['end_game'])]
    if 'season_type' in kwargs:
        if isinstance(kwargs['season_type'], int):
            stypes = set((kwargs['season_type']))
        else:
            stypes = set(kwargs['season_type'])
        data = pd.concat([data.Game // 10000 == stype for stype in stypes])
    else:
        data = pd.concat([data.Game // 10000 == stype for stype in (2, 3)])
    return data


def _filter_for_players(data, **kwargs):
    """
    Uses the players_on_ice, players_on_ice_for, players_on_ice_ag, acting_player, and receiving_player keyword
    arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit player-related kwargs
    """

    if 'acting_player' in kwargs:
        p = ss.player_as_id(kwargs['acting_player'])
        data = data[data.Actor == p]

    if 'receiving_player' in kwargs:
        p = ss.player_as_id(kwargs['receiving_player'])
        data = data[data.Recipient == p]

    if 'add_on_ice' in kwargs or 'players_on_ice' in kwargs or 'players_on_ice_for' in kwargs or \
                    'players_on_ice_ag' in kwargs:
        # Now we know we need to read TOI
        dflst = []
        for season in set(data.Season):
            temp = data[data.Season == season]
            for game in set(temp.Game):
                dflst.append(_join_on_ice_players_to_pbp(season, game, temp[temp.Game == game]))
        data2 = pd.concat(dflst)

        if 'players_on_ice' in kwargs:
            players = set()
            key = 'players_on_ice'
            if key in kwargs:
                if ss.check_types(kwargs[key]):
                    players.add(kwargs[key])
                else:
                    players = players.union(kwargs[key])
            players = {ss.player_as_id(p) for p in players}

            querystrings = []
            for hr in ('H', 'R'):
                for suf in ('1', '2', '3', '4', '5', '6', 'G'):
                    for p in players:
                        querystrings.append('{0:s}{1:s} == {2:d}'.format(hr, suf, p))
            querystring = ' | '.join(querystrings)
            data2 = data2.query(querystring)

        # TODO finish players_on_ice_for and _ag

        if 'add_on_ice' in kwargs and not kwargs['add_on_ice']:
            data = data2[data.columns]
        else:
            data = data2

    return data


def _join_on_ice_players_to_pbp(season, game, pbp=None, toi=None):
    """
    For the given season and game, returns pbp with on-ice players attached.
    :param season: int, the season
    :param game: int, the game
    :param pbp: df, the plays. If None, will read from file.
    :param toi: df, the shifts to join to plays. If None, will read from file.
    :return: df, pbp but augmented with on-ice players
    """

    if pbp is None:
        pbp = sg.get_parsed_pbp(season, game)
    if toi is None:
        toi = sg.get_parsed_toi(season, game)

    newpbp = pbp.merge(toi, how='left', on='Time')
    return newpbp


def _filter_for_team(data, **kwargs):
    """
    Uses the team, team_for, team_ag, home_team, and road_team keyword arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit team-related kwargs
    """

    if 'team' in kwargs:
        teamid = ss.team_as_id(kwargs['team'])
        data = data[(data.Home == teamid) | (data.Road == teamid)]
    if 'team_for' in kwargs:
        teamid = ss.team_as_id(kwargs['team_for'])
        data = data[data.Team == teamid]
    if 'team_ag' in kwargs:
        teamid = ss.team_as_id(kwargs['team_ag'])
        data = data[((data.Home == teamid) | (data.Road == teamid)) & (data.Team != teamid)]

    if 'home_team' in kwargs:
        teamid = ss.team_as_id(kwargs['home_team'])
        data = data[data.Home == teamid]
    if 'road_team' in kwargs:
        teamid = ss.team_as_id(kwargs['road_team'])
        data = data[data.Road == teamid]

    return data


def _seasons_to_read(**kwargs):
    """
    Method uses start_date, end_date, start_season, and end_season to infer seasons to read
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: set of int (seasons)
    """

    minseason = 2011
    maxseason = ss.get_current_season()

    if 'start_season' in kwargs:
        minseason = max(kwargs['start_season'], minseason)
    if 'start_date' in kwargs:
        minseason = max(ss.infer_season_from_date(kwargs['start_date']), minseason)

    if 'end_season' in kwargs:
        maxseason = min(kwargs['end_season'], maxseason)
    if 'end_date' in kwargs:
        maxseason = max(ss.infer_season_from_date(kwargs['end_date']), maxseason)

    return list(range(minseason, maxseason + 1))


def _teams_to_read(**kwargs):
    """
    Method concatenates unique values from keyword arguments named team, team_for, and team_ag
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a set of int (team IDs)
    """

    teamlst = set()
    for key in ('team', 'team_for', 'team_ag'):
        if key in kwargs:
            if isinstance(kwargs[key], str) or isinstance(kwargs[key], int):
                teamlst.add(ss.team_as_id(kwargs[key]))
            else:
                for val in kwargs[key]:
                    teamlst.add(ss.team_as_id(val))
    return teamlst


def get_5v5_player_game_toi(season, team):
    """
    Gets TOION and TOIOFF by game and player for given team in given season.
    :param season: int, the season
    :param team: int, team id
    :return: df with game, player, TOION, and TOIOFF
    """
    fives = ss.get_team_toi(season, team) \
        .query('TeamStrength == "5" & OppStrength == "5"') \
        .filter(items=['Game', 'Time', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5'])

    # Get TOI by game. This is to get TOIOFF
    time_by_game = fives[['Game', 'Time']].groupby('Game').count().reset_index().rename(columns={'Time': 'TeamTOI'})

    # Now get a long dataframe of individual TOI
    fives2 = fives[['Game', 'Time', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5']]
    fives_long = pd.melt(fives2, id_vars=['Time', 'Game'], value_vars=['Team1', 'Team2', 'Team3', 'Team4', 'Team5'],
                         var_name='Team', value_name='Player') \
        .drop('Team', axis=1)

    # Now, by player. First at a game level to get TOIOFF
    toi_by_player = fives_long.groupby(['Player', 'Game']).count() \
        .reset_index() \
        .rename(columns={'Time': 'TOION'}) \
        .merge(time_by_game, how='left', on='Game')
    toi_by_player.loc[:, 'TOION'] = toi_by_player.TOION / 3600
    toi_by_player.loc[:, 'TOIOFF'] = toi_by_player.TeamTOI / 3600 - toi_by_player.TOION

    return toi_by_player.rename(columns={'Player': 'PlayerID'})


def get_5v5_player_season_toi(season, team):
    """
    Gets TOION and TOIOFF by player for given team in given season.
    :param season: int, the season
    :param team: int, team id
    :return: df with game, player, TOION, and TOIOFF
    """
    toi_by_player = get_5v5_player_game_toi(season, team)
    toi_indiv = toi_by_player[['PlayerID', 'TOION', 'TOIOFF']].groupby('PlayerID').sum().reset_index()
    return toi_indiv


def generate_player_toion_toioff(season):
    """
    Generates TOION and TOIOFF at 5v5 for each player in this season.
    :param season: int, the season
    :return: df with columns Player, TOION, TOIOFF, and TOI60.
    """

    spinner = halo.Halo()
    spinner.start(text='Generating TOI60 for {0:d}'.format(season))

    team_by_team = []
    allteams = ss.get_teams_in_season(season)
    for i, team in enumerate(allteams):
        if os.path.exists(ss.get_team_toi_filename(season, team)):
            spinner.start(text='Generating TOI60 for {0:d} {1:s} ({2:d}/{3:d})'.format(
                season, ss.team_as_str(team), i + 1, len(allteams)))
            toi_indiv = get_5v5_player_season_toi(season, team)
            team_by_team.append(toi_indiv)
            spinner.stop()

    toi60 = pd.concat(team_by_team)
    toi60 = toi60.groupby('PlayerID').sum().reset_index()
    toi60.loc[:, 'TOI%'] = toi60.TOION / (toi60.TOION + toi60.TOIOFF)
    toi60.loc[:, 'TOI60'] = toi60['TOI%'] * 60

    return toi60


def get_player_positions():
    """
    Use to get player positions
    :return: df with colnames ID and position
    """

    return ss.get_player_ids_file()[['ID', 'Pos']]


def get_toicomp_file(season, force_create=False):
    """
    If you want to rewrite the TOI60 file, too, then run get_player_toion_toioff_file with force_create=True before
    running this method.
    :param season: int, the season
    :param force_create: bool, should this be read from file if possible, or created from scratch
    :return:
    """

    fname = get_toicomp_filename(season)
    if os.path.exists(fname) and not force_create:
        return pd.read_csv(fname)
    else:
        df = generate_toicomp(season)
        save_toicomp_file(df, season)
        return get_toicomp_file(season)


def get_toicomp_filename(season):
    """

    :param season: int, the season
    :return:
    """
    return os.path.join(ss.get_other_data_folder(), '{0:d}_toicomp.csv'.format(season))


def save_toicomp_file(df, season):
    """

    :param df:
    :param season: int, the season
    :return:
    """
    df.to_csv(get_toicomp_filename(season), index=False)


def generate_toicomp(season):
    """
    Generates toicomp at a player-game level
    :param season: int, the season
    :return: df,
    """

    spinner = halo.Halo()
    spinner.start(text='Generating TOICOMP for {0:d}'.format(season))

    team_by_team = []
    allteams = ss.get_teams_in_season(season)
    for i, team in enumerate(allteams):
        if os.path.exists(ss.get_team_toi_filename(season, team)):
            spinner.start(text='Generating TOICOMP for {0:d} {1:s} ({2:d}/{3:d})'.format(
                season, ss.team_as_str(team), i + 1, len(allteams)))

            qct = get_5v5_player_game_toicomp(season, team)
            if qct is not None:
                team_by_team.append(qct)
            spinner.stop()

    df = pd.concat(team_by_team)
    return df


def get_player_5v5_log(season, force_create=False):
    """

    :param season: int, the season
    :param force_create: bool, create from scratch even if it exists?
    :return:
    """
    fname = get_5v5_player_log_filename(season)
    if os.path.exists(fname) and not force_create:
        return feather.read_dataframe(fname)
    else:
        df = generate_5v5_player_log(season)
        save_5v5_player_log(df, season)
        return get_player_5v5_log(season)


def get_5v5_player_log_filename(season):
    """

    :param season: int, the season
    :return:
    """
    return os.path.join(ss.get_other_data_folder(), '{0:d}_player_5v5_log.feather'.format(season))


def save_5v5_player_log(df, season):
    """

    :param season: int, the season
    :return: nothing
    """
    return feather.write_dataframe(df, get_5v5_player_log_filename(season))


def get_5v5_player_game_boxcars(season, team):
    return pd.DataFrame({'PlayerID': [0], 'Game': [0]})


def get_5v5_player_game_toicomp(season, team):
    """
    Calculates data for QoT and QoC at a player-game level for given team in given season.
    :param season: int, the season
    :param team: int, team id
    :return: df with game, player,
    """

    toidf = ss.get_team_toi(season, team).drop_duplicates()
    toidf.loc[:, 'TeamStrength'] = toidf.TeamStrength.astype(str)
    toidf.loc[:, 'OppStrength'] = toidf.OppStrength.astype(str)
    # Filter to 5v5
    toidf = toidf[(toidf.TeamStrength == '5') & (toidf.OppStrength == '5')] \
        .drop({'FocusTeam', 'TeamG', 'OppG', 'Team6', 'Opp6', 'TeamScore', 'OppScore',
               'Team', 'Opp', 'Time', 'TeamStrength', 'OppStrength', 'Home', 'Road'},
              axis=1, errors='ignore')

    if len(toidf) > 0:
        df_for_qoc = toidf
        df_for_qot = toidf.assign(Opp1=toidf.Team1, Opp2=toidf.Team2,
                                  Opp3=toidf.Team3, Opp4=toidf.Team4, Opp5=toidf.Team5)

        qc1 = _long_on_player_and_opp(df_for_qoc)
        qc2 = _merge_toi60_position_calculate_comp(qc1, season, 'Comp')

        qt1 = _long_on_player_and_opp(df_for_qot)
        qt2 = _merge_toi60_position_calculate_comp(qt1, season, 'Team')

        qct = qc2.merge(qt2, how='inner', on=['Game', 'TeamPlayerID'])
        qct.loc[:, 'Team'] = team
        qct = qct.rename(columns={'TeamPlayerID': 'PlayerID'})
        return qct
    else:
        return None


def _long_on_player_and_opp(df):
    """
    A helper method for get_5v5_player_game_toicomp. Goes from standard format (which has one row per second) to
    long format (one row per player1-player2 pair)
    :param df: dataframe with game and players
    :return: dataframe, melted
    """

    # Melt opponents down. Group by Game, TeamPlayers, and Opponent, and take counts
    # Then melt by team players. Group by game, team player, and opp player, and sum counts
    df2 = pd.melt(df, id_vars=['Game', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5'],
                  value_vars=['Opp1', 'Opp2', 'Opp3', 'Opp4', 'Opp5'],
                  var_name='OppNum', value_name='OppPlayerID').drop('OppNum', axis=1).assign(Secs=1)
    df2 = df2.groupby(['Game', 'OppPlayerID', 'Team1',
                       'Team2', 'Team3', 'Team4', 'Team5']).sum().reset_index()
    df2 = pd.melt(df2, id_vars=['Game', 'OppPlayerID', 'Secs'],
                  value_vars=['Team1', 'Team2', 'Team3', 'Team4', 'Team5'],
                  var_name='TeamNum', value_name='TeamPlayerID').drop('TeamNum', axis=1)
    # Filter out self for team cases
    df2 = df2.query("TeamPlayerID != OppPlayerID")
    df2 = df2.groupby(['Game', 'TeamPlayerID', 'OppPlayerID']).sum().reset_index()
    return df2


def _merge_toi60_position_calculate_comp(df, season, suffix='Comp'):
    """
    Merges dataframe with toi60 and positions to calculate QoC or QoT by player and game.
    Used in get_5v5_player_game_toicomp
    :param df: dataframe with players and times faced
    :param suffix: use 'Comp' for QoC and 'Team' for QoT
    :return: a dataframe with QoC and QoT by player and game
    """

    toi60df = get_player_toion_toioff_file(season)
    posdf = get_player_positions()

    # Attach toi60 and positions, and calculate sums
    qoc = df.merge(toi60df, how='left', left_on='OppPlayerID', right_on='PlayerID') \
        .merge(posdf, how='left', left_on='OppPlayerID', right_on='ID') \
        .drop({'PlayerID', 'TOION', 'TOIOFF', 'TOI%', 'ID'}, axis=1)
    qoc.loc[:, 'Pos2'] = qoc.Pos.apply(
        lambda x: 'D' + suffix if x == 'D' else 'F' + suffix)  # There shouldn't be any goalies
    qoc.loc[:, 'TOI60Sum'] = qoc.Secs * qoc.TOI60
    qoc = qoc.drop('Pos', axis=1)
    qoc = qoc.drop({'OppPlayerID', 'TOI60'}, axis=1) \
        .groupby(['Game', 'TeamPlayerID', 'Pos2']).sum().reset_index()
    qoc.loc[:, suffix] = qoc.TOI60Sum / qoc.Secs
    qoc = qoc[['Game', 'TeamPlayerID', 'Pos2', suffix]] \
        .pivot_table(index=['Game', 'TeamPlayerID'], columns='Pos2', values=suffix).reset_index()
    return qoc


def get_5v5_player_game_shift_startend(season, team):
    return pd.DataFrame({'PlayerID': [0], 'Game': [0]})


def generate_5v5_player_log(season):
    """
    Takes the play by play and adds player 5v5 info to the master player log file, noting TOI, CF, etc.
    This takes awhile because it has to calculate TOICOMP.
    :param season: int, the season
    :return: nothing
    """
    spinner = halo.Halo()
    spinner.start(text='Generating player log for {0:d}'.format(season))

    to_concat = []

    # Recreate TOI60 file.
    _ = get_player_toion_toioff_file(season, force_create=True)

    for team in ss.get_teams_in_season(season):
        try:
            goals = get_5v5_player_game_boxcars(season, team)  # G, A1, A2, SOG, iCF
            cfca = get_5v5_player_game_cfca(season, team)  # CFON, CAON, CFOFF, CAOFF, and same for goals
            toi = get_5v5_player_game_toi(season, team)  # TOION and TOIOFF
            toicomp = get_5v5_player_game_toicomp(season, team)  # FQoC, F QoT, D QoC, D QoT, and respective Ns
            shifts = get_5v5_player_game_shift_startend(season, team)  # OZ, NZ, DZ, OTF-O, OTF-D, OTF-N

            temp = toi \
                .merge(cfca, how='left', on=['PlayerID', 'Game']) \
                .merge(toicomp.drop('Team', axis=1), how='left', on=['PlayerID', 'Game'])
            # .merge(goals, how='left', on=['PlayerID', 'Game']) \
            # .merge(shifts, how='left', on=['PlayerID', 'Game'])

            to_concat.append(temp)
        except Exception as e:
            print('Issue with generating game-by-game for', season, team)
            print(e)

    df = pd.concat(to_concat)
    for col in df.columns:
        df.loc[:, col] = pd.to_numeric(df[col])
    df = df[df.Game >= 20001]  # no preseason
    df = df[df.Game <= 30417]  # no ASG, WC, Olympics, etc
    for col in df:
        if df[col].isnull().sum() > 0:
            print('In player log, {0:s} has null values; filling with zeroes'.format(col))
            df.loc[:, col] = df[col].fillna(0)
    spinner.stop()
    return df


def get_5v5_player_game_cfca(season, team):
    """
    Gets CFON, CAON, CFOFF, and CAOFF by game for given team in given season.
    :param season: int, the season
    :param team: int, team id
    :return: df with game, player, CFON, CAON, CFOFF, and CAOFF
    """
    team = ss.team_as_id(team)
    # TODO create generate methods. Get methods check if file exists and if not, create anew (or overwrite)
    pbp = filter_for_corsi(ss.get_team_pbp(season, team))
    pbp.loc[:, 'TeamEvent'] = pbp.Team.apply(lambda x: 'CF' if x == team else 'CA')

    teamtotals = pbp[['Game', 'TeamEvent']] \
        .assign(Count=1) \
        .groupby(['Game', 'TeamEvent']).count().reset_index() \
        .pivot_table(index='Game', columns='TeamEvent', values='Count').reset_index() \
        .rename(columns={'CF': 'TeamCF', 'CA': 'TeamCA'})

    toi = ss.get_team_toi(season, team)
    toi = toi[['Game', 'Time', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5']].drop_duplicates()
    indivtotals = pbp.merge(toi, how='left', on=['Game', 'Time'])
    indivtotals = indivtotals[['Game', 'TeamEvent', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5']] \
        .melt(id_vars=['Game', 'TeamEvent'], value_vars=['Team1', 'Team2', 'Team3', 'Team4', 'Team5'],
              var_name='Temp', value_name='PlayerID') \
        .drop('Temp', axis=1) \
        .assign(Count=1) \
        .groupby(['Game', 'TeamEvent', 'PlayerID']).count().reset_index() \
        .pivot_table(index=['Game', 'PlayerID'], columns='TeamEvent', values='Count').reset_index() \
        .rename(columns={'CF': 'CFON', 'CA': 'CAON'})

    df = indivtotals.merge(teamtotals, how='inner', on='Game')
    for col in ['CFON', 'CAON', 'TeamCF', 'TeamCA']:
        if col not in df.columns:
            df.loc[:, col] = 0
        df.loc[:, col] = df[col].fillna(0)
    df.loc['CFOFF'] = df.TeamCF - df.CFON
    df.loc['CAOFF'] = df.TeamCA - df.CAON
    return df


def _convert_to_all_combos(df, fillval=0, *args):
    """
    This method takes a dataframe and makes sure all possible combinations of given arguments are present.
    For example, if you want df to have all combos of P1 and P2, it will create a dataframe with all possible combos,
    left join existing dataframe onto that, and return that df. Uses fillval to fill *all* non-key columns.
    :param df: the pandas dataframe
    :param fillval: obj, the value with which to fill. Default fill is 0
    :param args: str, column names, or tuples of combinations of column names
    :return: df with all combos of columns specified
    """
    args = set(args)
    if len(args) == 1:
        df.loc[:, list(args)[0]] = df[list(args)[0]].fillna(fillval)
        return df  # Nothing else to do here

    dfs_with_unique = []
    for combo in args:
        if isinstance(combo, str):
            tempdf = df[[combo]].drop_duplicates()
        else:
            tempdf = df[list(combo)].drop_duplicates()
        dfs_with_unique.append(tempdf.assign(JoinKey=1))

    # Now join all these dfs together
    complete_df = functools.reduce(lambda x, y: pd.merge(x, y, how='inner', on='JoinKey'), dfs_with_unique)

    # And left join on original
    all_key_cols = set()
    for i in range(len(dfs_with_unique)):
        all_key_cols = all_key_cols.union(set(dfs_with_unique[i].columns))
    final_df = complete_df.merge(df.assign(JoinKey=1), how='left', on=list(all_key_cols)).drop('JoinKey', axis=1)

    # Fill in values
    for col in final_df.columns:
        if col not in all_key_cols:
            final_df.loc[:, col] = final_df.loc[:, col].fillna(fillval)

    return final_df


def get_player_toi(season, game, pos=None, homeroad='H'):
    """
    Returns a df listing 5v5 ice time for each player for specified team.
    :param season: int, the game
    :param game: int, the season
    :param pos: specify 'L', 'C', 'R', 'D' or None for all
    :param homeroad: str, 'H' for home or 'R' for road
    :return: pandas df with columns Player, Secs
    """

    # TODO this isn't working properly for in-progress games. Or maybe it's my scraping earlier.

    toi = sg.get_parsed_toi(season, game)
    posdf = get_player_positions()

    fives = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "5")]
    cols_to_keep = ['Time'] + ['{0:s}{1:d}'.format(homeroad, i + 1) for i in range(5)]
    players = fives[cols_to_keep] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .groupby('PlayerID').count().reset_index() \
        .rename(columns={'Time': 'Secs'}) \
        .merge(posdf, how='left', left_on='PlayerID', right_on='ID') \
        .drop('ID', axis=1) \
        .sort_values('Secs', ascending=False)
    if pos is not None:
        if pos == 'F':
            players = players.query('Pos != "D"')
        else:
            players = players.query('Pos == "{0:s}"'.format(pos))
    return players


def get_line_combos(season, game, homeroad='H'):
    """
    Returns a df listing the 5v5 line combinations used in this game for specified team, and time they each played together
    :param season: int, the game
    :param game: int, the season
    :param homeroad: str, 'H' for home or 'R' for road
    :return: pandas dataframe with columns P1, P2, P3, Secs. May contain duplicates
    """

    toi = sg.get_parsed_toi(season, game)
    pos = get_player_positions()

    fives = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "5")]
    cols_to_keep = ['Time'] + ['{0:s}{1:d}'.format(homeroad, i + 1) for i in range(5)]
    players = fives[cols_to_keep] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .merge(pos, how='left', left_on='PlayerID', right_on='ID') \
        .query('Pos != "D"') \
        .drop({'Pos', 'ID'}, axis=1)
    wide = players.merge(players, how='inner', on='Time', suffixes=['1', '2']) \
        .merge(players, how='inner', on='Time') \
        .rename(columns={'PlayerID': 'PlayerID3'}) \
        .query('PlayerID1 != PlayerID2 & PlayerID1 != PlayerID3 & PlayerID2 != PlayerID3')
    counts = wide.groupby(['PlayerID1', 'PlayerID2', 'PlayerID3']).count().reset_index() \
        .rename(columns={'Time': 'Secs'})
    return counts


def get_pairings(season, game, homeroad='H'):
    """
    Returns a df listing the 5v5 pairs used in this game for specified team, and time they each played together
    :param season: int, the game
    :param game: int, the season
    :param homeroad: str, 'H' for home or 'R' for road
    :return: pandas dataframe with columns P1, P2, Secs. May contain duplicates
    """

    toi = sg.get_parsed_toi(season, game)
    pos = get_player_positions()

    fives = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "5")]
    cols_to_keep = ['Time'] + ['{0:s}{1:d}'.format(homeroad, i + 1) for i in range(5)]
    players = fives[cols_to_keep] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .merge(pos, how='left', left_on='PlayerID', right_on='ID') \
        .query('Pos == "D"') \
        .drop({'Pos', 'ID'}, axis=1)
    wide = players.merge(players, how='inner', on='Time', suffixes=['1', '2']) \
        .query('PlayerID1 != PlayerID2')
    counts = wide.groupby(['PlayerID1', 'PlayerID2']).count().reset_index() \
        .rename(columns={'Time': 'Secs'})
    return counts


def get_game_h2h_toi(season, game):
    """
    This method gets H2H TOI at 5v5 for the given game.
    :param season: int, the season
    :param game: int, the game
    :return: a df with [P1, P1Team, P2, P2Team, TOI]. Entries will be duplicated (one with given P as P1, another as P2)
    """
    # TODO add strength arg
    toi = sg.get_parsed_toi(season, game)
    fives = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "5")]
    home = fives[['Time', 'H1', 'H2', 'H3', 'H4', 'H5']] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .assign(Team='H')
    road = fives[['Time', 'R1', 'R2', 'R3', 'R4', 'R5']] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .assign(Team='R')

    hh = home.merge(home, how='inner', on='Time', suffixes=['1', '2'])
    hr = home.merge(road, how='inner', on='Time', suffixes=['1', '2'])
    rh = road.merge(home, how='inner', on='Time', suffixes=['1', '2'])
    rr = road.merge(road, how='inner', on='Time', suffixes=['1', '2'])

    pairs = pd.concat([hh, hr, rh, rr]) \
        .assign(Secs=1) \
        .drop('Time', axis=1) \
        .groupby(['PlayerID1', 'PlayerID2', 'Team1', 'Team2']).count().reset_index()

    # One last to-do: make sure I have all possible pairs of players covered

    allpairs = _convert_to_all_combos(pairs, 0, ('PlayerID1', 'Team1'), ('PlayerID2', 'Team2'))

    allpairs.loc[:, 'Min'] = allpairs.Secs / 60
    return allpairs


def filter_for_corsi(pbp):
    """
    Filters given dataframe for 5v5 goal, shot, miss, and block events
    :param pbp: a dataframe with columns Event + HomeStrength + RoadStrength (or TeamStrength + OppStrength)
    :return: pbp, filtered for 5v5 corsi events
    """
    colnames = set(pbp.columns)
    if 'HomeStrength' in colnames and 'RoadStrength' in colnames:
        fives = pbp[(pbp.HomeStrength == "5") & (pbp.RoadStrength == "5")]
    elif 'TeamStrength' in colnames and 'OppStrength' in colnames:
        fives = pbp[(pbp.TeamStrength == "5") & (pbp.OppStrength == "5")]
    else:
        fives = pbp

    corsi = fives[fives.Event.apply(lambda x: x in {"Goal", "Shot", "Missed Shot", "Blocked Shot"})]

    return corsi


def get_game_h2h_corsi(season, game):
    """
    This method gets H2H Corsi at 5v5 for the given game.
    :param season: int, the season
    :param game: int, the game
    :return: a df with [P1, P1Team, P2, P2Team, CF, CA, C+/-]. Entries will be duplicated, as with get_game_h2h_toi.
    """
    # TODO add strength arg
    toi = sg.get_parsed_toi(season, game)
    pbp = sg.get_parsed_pbp(season, game)
    # toi.to_csv('/Users/muneebalam/Desktop/toi.csv')
    # pbp.to_csv('/Users/muneebalam/Desktop/pbp.csv')
    # pbp.loc[:, 'Event'] = pbp.Event.apply(lambda x: ss.convert_event(x))
    pbp = pbp[['Time', 'Event', 'Team']] \
        .merge(toi[['Time', 'R1', 'R2', 'R3', 'R4', 'R5', 'H1', 'H2', 'H3', 'H4', 'H5',
                    'HomeStrength', 'RoadStrength']], how='inner', on='Time')
    corsi = filter_for_corsi(pbp).drop(['HomeStrength', 'RoadStrength'], axis=1)

    hometeam = ss.get_home_team(season, game)
    # Add HomeCorsi which will be 1 or -1. Need to separate out blocks because they're credited to defending team
    # Never mind, switched block attribution at time of parsing, so we're good now
    corsi.loc[:, 'HomeCorsi'] = corsi.Team.apply(lambda x: 1 if x == hometeam else -1)

    corsipm = corsi[['Time', 'HomeCorsi']]

    home = corsi[['Time', 'H1', 'H2', 'H3', 'H4', 'H5']] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1)
    road = corsi[['Time', 'R1', 'R2', 'R3', 'R4', 'R5']] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1)

    hh = home.merge(home, how='inner', on='Time', suffixes=['1', '2']).assign(Team1='H', Team2='H')
    hr = home.merge(road, how='inner', on='Time', suffixes=['1', '2']).assign(Team1='H', Team2='R')
    rh = road.merge(home, how='inner', on='Time', suffixes=['1', '2']).assign(Team1='R', Team2='H')
    rr = road.merge(road, how='inner', on='Time', suffixes=['1', '2']).assign(Team1='R', Team2='R')

    pairs = pd.concat([hh, hr, rh, rr]) \
        .merge(corsipm, how='inner', on='Time') \
        .drop('Time', axis=1) \
        .groupby(['PlayerID1', 'PlayerID2', 'Team1', 'Team2']).sum().reset_index()
    pairs.loc[pairs.Team1 == 'R', 'HomeCorsi'] = pairs.loc[pairs.Team1 == 'R', 'HomeCorsi'] * -1
    allpairs = _convert_to_all_combos(pairs, 0, ('PlayerID1', 'Team1'), ('PlayerID2', 'Team2'))
    return allpairs


def time_to_mss(sectime):
    """
    Converts a number of seconds to m:ss format
    :param sectime: int, a number of seconds
    :return: str, sectime in m:ss
    """
    n_min = int(sectime / 60)
    n_sec = int(sectime % 60)
    if n_sec == 0:
        return '{0:d}:00'.format(n_min)
    elif n_sec < 10:
        return '{0:d}:0{1:d}'.format(n_min, n_sec)
    else:
        return '{0:d}:{1:d}'.format(n_min, n_sec)


def period_time_to_elapsed(df, period_colname='Period', time_colname='Time', time_format='elapsed'):
    """
    Adds a column to df called Elapsed.
    :param df: pandas dataframe
    :param periodcol: str, column containing period (ints)
    :param time_colname: str, column containing time in m:ss format.
    :param time_format: str, 'elapsed' or 'remaining'. Latter not suitable for regular season.
    :return: dataframe with extra column noting seconds elapsed in game
    """

    newdf_minsec = df[time_colname].str.split(':', expand=True)
    newdf_minsec.columns = ['Min', 'Sec']
    newdf_minsec.loc[:, 'TimeElapsed_P'] = (df[period_colname] - 1) * 1200
    if time_format == 'elapsed':
        newdf_minsec.loc[:, 'TimeElapsed'] = newdf_minsec['TimeElapsed_P'] + newdf_minsec.Min * 60 + newdf_minsec.Sec
        df.loc[:, 'Elapsed'] = newdf_minsec.TimeElapsed
    elif time_format == 'remaining':
        newdf_minsec.loc[:, 'TimeElapsed'] = newdf_minsec['TimeElapsed_P'] + \
                                             1200 - (newdf_minsec.Min * 60 + newdf_minsec.Sec)
        df.loc[:, 'Elapsed'] = newdf_minsec.TimeElapsed
    return df


def add_on_ice_players_to_file(filename, *args, **kwargs):
    """
    A method to add on-ice players to each row.
    :param filename: str, path to file. Can read csv or xlsx (first sheet only)
    :param period_colname: str, name of column containing period (ints)
    :param time_colname: str, name of column containing time in m:ss
    :param time_format: str, use 'elapsed' (preferred) or 'remaining' (latter may not work for regular season)
    :param faceoff_indicator: bool. This is suitable for events like shots. For faceoffs, specify True.
    This is because TOI is recorded in a non-overlapping way. At the time of the faceoff, the on-ice players listed
    are the players still on from the previous time. To get new players coming onto the ice, if this arg is True,
    will add 1 to times before joining.
    :return: nothing
    """
    # TODO handle date specification instead of season-game

    if filename[-4:] == '.csv':
        df = pd.read_csv(filename)
    elif filename[-5:] == '.xlsx':
        df = pd.read_excel(filename)

    newdf = add_on_ice_players_to_df(df, *args, **kwargs)
    new_filename = filename[:filename.rfind('.')] + '_on_ice' + filename[filename.rfind('.'):]
    if filename[-4:] == '.csv':
        df.to_csv(filename, index=False)
    elif filename[-5:] == '.xlsx':
        df.to_excel(filename, index=False)


def add_on_ice_players_to_df(df, season_colname=None, game_colname='Game', period_colname='Period',
                             time_colname='Time', time_format='elapsed', faceoff_indicator=False):
    """
    A method to add on-ice players to each row.
    :param df: a dataframe
    :param season_colname: str, name of column containing season. Defaults to assuming all are current season
    :param game_colname: str, name of column containing game
    :param period_colname: str, name of column containing period (ints)
    :param time_colname: str, name of column containing time in m:ss
    :param time_format: str, use 'elapsed' (preferred) or 'remaining' (latter may not work for regular season)
    :param faceoff_indicator: bool. This is suitable for events like shots. For faceoffs, specify True.
    This is because TOI is recorded in a non-overlapping way. At the time of the faceoff, the on-ice players listed
    are the players still on from the previous time. To get new players coming onto the ice, if this arg is True,
    will join to players on ice one second after.
    :return: nothing
    """

    df = period_time_to_elapsed(df, period_colname, time_colname, time_format)
    if season_colname is None or season_colname not in df.columns:
        df.loc[:, 'Season'] = ss.get_current_season()
        season_colname = 'Season'

    newdf = []
    seasons_and_games = df[[season_colname, game_colname]].drop_duplicates()
    for season, game in seasons_and_games.iterrows():
        tempdf = df[(df[season_colname] == season) & (df[game_colname] == game)]
        toi = sg.get_parsed_toi(season, game)
        if faceoff_indicator:
            toi.loc[:, 'Time'] = toi.Time - 1
        tempdf = tempdf.merge(toi.rename(columns={'Time': 'Elapsed'}), how='left', on='Elapsed')

        newdf.append(tempdf)

    newdf = pd.concat(newdf)
    newdf = player_columns_to_name(newdf)
    return newdf


def player_columns_to_name(df, columns=None):
    """
    Takes a dataframe and transforms specified columns of player IDs into names.
    If no columns provided, searches for defaults: H1, H2, H3, H4, H5, H6, HG (and same seven with R)
    :param df: A dataframe
    :param columns: a list of strings, or None
    :return: df, dataframe with same column names, but columns now names instead of IDs
    """

    if columns is None:
        columns = set(['{0:s}{1:s}'.format(hr, i) for hr in ['H', 'R'] for i in ['1', '2', '3', '4', '5', '6', 'G']])
    colnames = set(df.columns)
    players = ss.get_player_ids_file()[['ID', 'Name']]

    newdf = pd.DataFrame(index=df.index)
    for col in colnames:
        if col in columns:
            newdf = newdf.merge(players, how='left', left_on=col, right_on='ID') \
                .drop([col, 'ID'], axis=1) \
                .rename(columns={'ID': col})
        else:
            newdf.loc[:, col] = df[col]

    return newdf
