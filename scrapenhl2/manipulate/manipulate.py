import functools
import os
import os.path
import os.path

import feather
import pandas as pd

from scrapenhl2.scrape import general_helpers as helpers
from scrapenhl2.scrape import organization, schedules, teams, parse_pbp, parse_toi, players, events, team_info, scrape_pbp


def get_player_toion_toioff_filename(season):
    """

    :param season: int, the season
    :return:
    """
    return os.path.join(organization.get_other_data_folder(), '{0:d}_season_toi60.csv'.format(season))


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
    # TODO finish
    # Read from team logs. Since I store by team, first, read relevant teams' logs
    all_teams_to_read = _teams_to_read(**kwargs)
    all_seasons_to_read = _seasons_to_read(**kwargs)

    for season in all_seasons_to_read:
        df = pd.concat([teams.get_team_pbp(season, team) for team in all_teams_to_read])
        df = _filter_for_team(df, **kwargs)

        df = _filter_for_games(df, **kwargs)

        df = _filter_for_times(df, **kwargs)

        df = _filter_for_strengths(df, **kwargs)

        df = _filter_for_event_types(df, *args)

        # This could take longest, since it involved reading TOI, so leave it until the end
        df = _filter_for_players(df, **kwargs)
    return df


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
        dflst.append(data[data.Event2 == events.get_event_longname(arg)])
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
        p = players.player_as_id(kwargs['acting_player'])
        data = data[data.Actor == p]

    if 'receiving_player' in kwargs:
        p = players.player_as_id(kwargs['receiving_player'])
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
            playersonice = set()
            key = 'players_on_ice'
            if key in kwargs:
                if helpers.check_types(kwargs[key]):
                    playersonice.add(kwargs[key])
                else:
                    playersonice = playersonice.union(kwargs[key])
            playersonice = {playersonice.player_as_id(p) for p in playersonice}

            querystrings = []
            for hr in ('H', 'R'):
                for suf in ('1', '2', '3', '4', '5', '6', 'G'):
                    for p in playersonice:
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
        pbp = parse_pbp.get_parsed_pbp(season, game)
    if toi is None:
        toi = parse_toi.get_parsed_toi(season, game)

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
        teamid = team_info.team_as_id(kwargs['team'])
        data = data[(data.Home == teamid) | (data.Road == teamid)]
    if 'team_for' in kwargs:
        teamid = team_info.team_as_id(kwargs['team_for'])
        data = data[data.Team == teamid]
    if 'team_ag' in kwargs:
        teamid = team_info.team_as_id(kwargs['team_ag'])
        data = data[((data.Home == teamid) | (data.Road == teamid)) & (data.Team != teamid)]

    if 'home_team' in kwargs:
        teamid = team_info.team_as_id(kwargs['home_team'])
        data = data[data.Home == teamid]
    if 'road_team' in kwargs:
        teamid = team_info.team_as_id(kwargs['road_team'])
        data = data[data.Road == teamid]

    return data


def _seasons_to_read(**kwargs):
    """
    Method uses start_date, end_date, start_season, and end_season to infer seasons to read
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: set of int (seasons)
    """

    minseason = 2011
    maxseason = schedules.get_current_season()

    if 'start_season' in kwargs:
        minseason = max(kwargs['start_season'], minseason)
    if 'start_date' in kwargs:
        minseason = max(helpers.infer_season_from_date(kwargs['start_date']), minseason)

    if 'end_season' in kwargs:
        maxseason = min(kwargs['end_season'], maxseason)
    if 'end_date' in kwargs:
        maxseason = max(helpers.infer_season_from_date(kwargs['end_date']), maxseason)

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
                teamlst.add(team_info.team_as_id(kwargs[key]))
            else:
                for val in kwargs[key]:
                    teamlst.add(team_info.team_as_id(val))
    return teamlst


def get_5v5_player_game_toi(season, team):
    """
    Gets TOION and TOIOFF by game and player for given team in given season.
    :param season: int, the season
    :param team: int, team id
    :return: df with game, player, TOION, and TOIOFF
    """
    fives = teams.get_team_toi(season, team) \
        .query('TeamStrength == "5" & OppStrength == "5"') \
        .filter(items=['Game', 'Time', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5'])

    # Get TOI by game. This is to get TOIOFF
    time_by_game = fives[['Game', 'Time']].groupby('Game').count().reset_index().rename(columns={'Time': 'TeamTOI'})

    # Now get a long dataframe of individual TOI
    fives2 = fives[['Game', 'Time', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5']]
    fives_long = pd.melt(fives2, id_vars=['Time', 'Game'], value_vars=['Team1', 'Team2', 'Team3', 'Team4', 'Team5'],
                         var_name='Team', value_name='Player') \
        .drop('Team', axis=1)

    fives_long = merge_onto_all_team_games_and_zero_fill(fives_long, season, team)

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

    team_by_team = []
    allteams = schedules.get_teams_in_season(season)
    for i, team in enumerate(allteams):
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


def get_player_positions():
    """
    Use to get player positions
    :return: df with colnames ID and position
    """

    return players.get_player_ids_file()[['ID', 'Pos']]


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
    return os.path.join(organization.get_other_data_folder(), '{0:d}_toicomp.csv'.format(season))


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

    team_by_team = []
    allteams = team_info.get_teams_in_season(season)
    for i, team in enumerate(allteams):
        if os.path.exists(teams.get_team_toi_filename(season, team)):
            print('Generating TOICOMP for {0:d} {1:s} ({2:d}/{3:d})'.format(
                season, team_info.team_as_str(team), i + 1, len(allteams)))

            qct = get_5v5_player_game_toicomp(season, team)
            if qct is not None:
                team_by_team.append(qct)

    df = pd.concat(team_by_team)
    return df


def get_5v5_player_log(season, force_create=False):
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
        return get_5v5_player_log(season)


def get_5v5_player_log_filename(season):
    """

    :param season: int, the season
    :return:
    """
    return os.path.join(organization.get_other_data_folder(), '{0:d}_player_5v5_log.feather'.format(season))


def save_5v5_player_log(df, season):
    """

    :param season: int, the season
    :return: nothing
    """
    return feather.write_dataframe(df, get_5v5_player_log_filename(season))


def filter_for_team(pbp, team):
    """
    Filters dataframe for rows where Team == team

    :param pbp: dataframe. Needs to have column Team
    :param team: int or str, team ID or name

    :return: dataframe with rows filtered
    """
    return pbp[pbp.Team == team_info.team_as_id(team)]


def count_by_keys(df, *args):
    """
    A convenience method that isolates specified columns in the dataframe and gets counts. Drops when keys have NAs.

    :param df: dataframe
    :param args: str, column names in dataframe

    :return: df, dataframe with each of args and an additional column, Count
    """
    args = list(args)
    return df[args].dropna().assign(Count=1).groupby(args).count().reset_index()


def get_5v5_player_game_boxcars(season, team):
    df = teams.get_team_pbp(season, team)
    fives = filter_for_five_on_five(df)
    fives = filter_for_team(fives, team)

    # iCF
    icf = count_by_keys(filter_for_corsi(fives), 'Game', 'Actor') \
        .rename(columns={'Actor': 'PlayerID', 'Count': 'iCF'})

    # iFF
    iff = count_by_keys(filter_for_fenwick(fives), 'Game', 'Actor') \
        .rename(columns={'Actor': 'PlayerID', 'Count': 'iFF'})

    # iSOG
    isog = count_by_keys(filter_for_sog(fives), 'Game', 'Actor') \
        .rename(columns={'Actor': 'PlayerID', 'Count': 'iSOG'})

    # iG
    goals = filter_for_goals(fives)
    ig = count_by_keys(goals, 'Game', 'Actor') \
        .rename(columns={'Actor': 'PlayerID', 'Count': 'iG'})

    # iA1--use Recipient column
    primaries = count_by_keys(goals, 'Game', 'Recipient') \
        .rename(columns={'Recipient': 'PlayerID', 'Count': 'iA1'})

    # iA1
    secondaries = goals[['Game', 'Note']]
    # Extract using regex: ...assists: [stuff] (num), [stuff] (num)
    # The first "stuff" is A1, second is A2. Nums are number of assists to date in season
    secondaries.loc[:, 'Player'] = secondaries.Note.str.extract('assists: .*\(\d+\),\s(.*)\s\(\d+\)')
    secondaries = count_by_keys(secondaries, 'Game', 'Player') \
        .rename(columns={'Count': 'iA2'})
    # I also need to change these to player IDs. Use iCF for help.
    # Assume single team won't have 2 players with same name in same season
    playerlst = icf[['PlayerID']] \
        .merge(players.get_player_ids_file().rename(columns={'ID': 'PlayerID'}),
               how='left', on='PlayerID')
    secondaries.loc[:, 'PlayerID'] = players.playerlst_as_id(secondaries.Player, True, playerlst)
    secondaries = secondaries[['Game', 'PlayerID', 'iA2']]

    boxcars = ig.merge(primaries, how='outer', on=['Game', 'PlayerID']) \
        .merge(secondaries, how='outer', on=['Game', 'PlayerID']) \
        .merge(isog, how='outer', on=['Game', 'PlayerID']) \
        .merge(iff, how='outer', on=['Game', 'PlayerID']) \
        .merge(icf, how='outer', on=['Game', 'PlayerID'])

    boxcars = merge_onto_all_team_games_and_zero_fill(boxcars, season, team)

    for col in boxcars.columns:
        boxcars.loc[:, col] = boxcars[col].fillna(0)

    return boxcars


def get_5v5_player_game_toicomp(season, team):
    """
    Calculates data for QoT and QoC at a player-game level for given team in given season.
    :param season: int, the season
    :param team: int, team id
    :return: df with game, player,
    """

    toidf = teams.get_team_toi(season, team).drop_duplicates()
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
        qc2 = _merge_toi60_position_calculate_sums(qc1, season, 'Comp')

        qt1 = _long_on_player_and_opp(df_for_qot)
        qt2 = _merge_toi60_position_calculate_sums(qt1, season, 'Team')

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


def _merge_toi60_position_calculate_sums(df, season, suffix='Comp'):
    """
    Merges dataframe with toi60 and positions to calculate sums for QoC or QoT by player and game.
    The reason this method doesn't calculate QoC and QoT is because you may want to sum over games.
    So it gives you the sum of TOI, and the N. Just sum over the games you want and divide TOI by N to get QoC/QoT.

    Used in get_5v5_player_game_toicomp.

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

    sums = qoc.drop('Secs', axis=1)
    sums.loc[:, 'Pos2'] = sums.Pos2.apply(lambda x: x + 'Sum')
    sums = sums.pivot_table(index=['Game', 'TeamPlayerID'], columns='Pos2', values='TOI60Sum').reset_index()

    ns = qoc.drop('TOI60Sum', axis=1)
    ns.loc[:, 'Pos2'] = ns.Pos2.apply(lambda x: x + 'N')
    ns = ns.pivot_table(index=['Game', 'TeamPlayerID'], columns='Pos2', values='Secs').reset_index()

    assert len(sums) == len(ns)

    return sums.merge(ns, how='inner', on=['Game', 'TeamPlayerID'])


def _retrieve_start_end_times(toidf):
    """
    Converts given dataframe, with one row per second, into one per player-shift

    :param toidf: dataframe at the season level

    :return: dataframe
    """
    df = toidf.drop({'FocusTeam', 'Home', 'Opp1', 'Opp2', 'Opp3', 'Opp4', 'Opp5', 'Opp6', 'OppG',
                     'OppScore', 'OppStrength', 'TeamScore', 'TeamStrength', 'Road'}, axis=1, errors='ignore') \
            .melt(id_vars=['Time', 'Game'], var_name='P', value_name='PlayerID') \
            .drop('P', axis=1) \
            .drop_duplicates() \
            .dropna()  # to get rid of NA Team6s, for example. Also, TODO: Why did I have duplicates here?

    # Mid-shift seconds need to be filtered out
    # To do that, add 1 to time and left join. Shift end is where value was not joined
    # Subtract 1 from time and left join. Shift start is where value was not joined

    df.loc[:, 'ShiftUp'] = df.Time + 1
    df.loc[:, 'ShiftDown'] = df.Time - 1
    df = df[['PlayerID', 'Game', 'Time']] \
        .merge(df[['PlayerID', 'Game', 'ShiftUp']].rename(columns={'ShiftUp': 'Time'}).assign(ShiftStart=0),
               how='left', on=['Time', 'Game', 'PlayerID']) \
        .merge(df[['PlayerID', 'Game', 'ShiftDown']].rename(columns={'ShiftDown': 'Time'}).assign(ShiftEnd=0),
               how='left', on=['Time', 'Game', 'PlayerID'])
    df.loc[:, 'ShiftStart'] = df.ShiftStart.fillna(1)
    df.loc[:, 'ShiftEnd'] = df.ShiftEnd.fillna(1)

    # Period starts and ends also need to be considered
    df.loc[df.Time % 1200 == 0, 'ShiftEnd'] = 1
    df.loc[df.Time % 1200 == 1, 'ShiftStart'] = 1

    df = df[(df.ShiftStart == 1) | (df.ShiftEnd == 1)] \
        .sort_values(['PlayerID', 'Game', 'Time'])
    df.loc[:, 'ShiftIndex'] = df.ShiftStart.cumsum()
    assert df.ShiftIndex.value_counts().values.max() == 2  # only two rows per shift

    shifts = df[['PlayerID', 'Game', 'ShiftIndex']].drop_duplicates()
    starts = df[df.ShiftStart == 1][['PlayerID', 'Game', 'Time', 'ShiftIndex']].rename(columns={'Time': 'StartTime'})
    ends = df[df.ShiftEnd == 1][['PlayerID', 'Game', 'Time', 'ShiftIndex']].rename(columns={'Time': 'EndTime'})
    shifts = shifts.merge(starts, how='left', on=['PlayerID', 'Game', 'ShiftIndex']) \
        .merge(ends, how='left', on=['PlayerID', 'Game', 'ShiftIndex'])

    return shifts


def get_5v5_player_game_shift_startend(season, team):
    """
    Generates shift starts and ends for shifts that start and end at 5v5--OZ, DZ, NZ, OtF.

    :param season: int, the season
    :param team: int or str, the team

    :return: dataframe with shift starts and ends
    """

    team = team_info.team_as_id(team)

    # First, turn TOI into start and end times
    teamtoi = teams.get_team_toi(season, team)
    shifts = _retrieve_start_end_times(teamtoi)

    # Now join faceoffs
    teamfo = filter_for_event_types(teams.get_team_pbp(season, team), 'Faceoff')[['Game', 'Time', 'X', 'Y', 'Team']]
    teamfo.loc[:, 'StartWL'] = teamfo.Team.apply(lambda x: 'W' if x == team else 'L')
    teamfo = teamfo.drop('Team', axis=1)

    foshifts = shifts.merge(
        teamfo.rename(columns={'X': 'EndX', 'Y': 'EndY', 'Time': 'EndTime'}).drop('StartWL', axis=1),
        how='left', on=['Game', 'EndTime'])

    teamfo.loc[:, 'Time'] = teamfo.Time + 1  # I add 1 to shift start times, so add 1 to faceoff times
    foshifts = foshifts.merge(
        teamfo.rename(columns={'X': 'StartX', 'Y': 'StartY', 'Time': 'StartTime'}),
        how='left', on=['Game', 'StartTime'])

    # Add locations
    directions = get_directions_for_xy_for_season(season, team)
    foshifts = infer_zones_for_faceoffs(foshifts, directions, 'StartX', 'StartY', 'StartTime') \
        .rename(columns={'FacLoc': 'Start'})
    foshifts.loc[:, 'Start'] = foshifts.Start.fillna('S-OtF')
    foshifts = infer_zones_for_faceoffs(foshifts, directions, 'EndX', 'EndY', 'EndTime') \
        .rename(columns={'FacLoc': 'End'})
    foshifts.loc[:, 'End'] = foshifts.End.fillna('E-OtF')

    # Filter out shifts that don't both start and end at 5v5
    fives = filter_for_five_on_five(teamtoi)[['Time', 'Game']]
    fiveshifts = foshifts.merge(fives.rename(columns={'Time': 'StartTime'}), how='inner', on=['StartTime', 'Game']) \
        .merge(fives.rename(columns={'Time': 'EndTime'}), how='inner', on=['EndTime', 'Game'])

    # Get counts and go long to wide

    starts = fiveshifts[['PlayerID', 'Game', 'StartWL', 'Start']].assign(Count=1)
    starts.loc[starts.StartWL.notnull(), 'Start'] = starts.Start.str.cat(starts.StartWL, sep='_')
    starts.drop('StartWL', axis=1, inplace=True)
    starts_w = starts.groupby(['PlayerID', 'Game', 'Start']).count().reset_index() \
        .pivot_table(index=['PlayerID', 'Game'], columns='Start', values='Count').reset_index()

    ends = fiveshifts[['PlayerID', 'Game', 'End']].assign(Count=1) \
        .groupby(['PlayerID', 'Game', 'End']).count().reset_index() \
        .pivot_table(index=['PlayerID', 'Game'], columns='End', values='Count').reset_index()

    finalshifts = starts_w.merge(ends, how='outer', on=['PlayerID', 'Game'])
    for col in finalshifts:
        finalshifts.loc[:, col] = finalshifts[col].fillna(0)

    return finalshifts


def get_directions_for_xy_for_season(season, team):
    """
    Gets directions for team specified using get_directions_for_xy_for_game

    :param season: int, the season
    :param team: int or str, the team

    :return: dataframe
    """
    sch = schedules.get_team_schedule(season, team) \
        .query('Status == "Final" & Game >= 20001')[['Game', 'Home', 'Road']]

    lrswitch = {'left': 'right', 'right': 'left', 'N/A': 'N/A'}

    game_to_directions = {}
    for index, game, home, road in sch.itertuples():
        try:
            dirdct = get_directions_for_xy_for_game(season, game)
        except Exception as e:
            print('Issue getting team directions for', season, game)
            print(e, e.args)

        game_to_directions[game] = []
        for period, direction in dirdct.items():
            if team == home:
                game_to_directions[game].append(direction)
            else:
                game_to_directions[game].append(lrswitch[direction])
    games = []
    periods = []
    directions = []
    for game in game_to_directions:
        for period, direction in enumerate(game_to_directions[game]):
            games.append(game)
            periods.append(period + 1)
            directions.append(direction)
    df = pd.DataFrame({'Game': games, 'Period': periods, 'Direction': directions})
    return df


def get_directions_for_xy_for_game(season, game):
    """
    It doesn't seem like there are rules for whether positive X in XY event locations corresponds to offensive zone
    events, for example. Best way is to use fields in the the json.

    :param season: int, the season
    :param game: int, the game

    :return: dict indicating which direction home team is attacking by period
    """

    json = scrape_pbp.get_raw_pbp(season, game)

    minidict = helpers.try_to_access_dict(json, 'liveData', 'linescore', 'periods')
    if minidict is None:
        # for games with no data, fill in N/A
        periods = {pernum: 'N/A' for pernum in range(1, 8)}
        return periods

    periods = {}
    for i in range(len(minidict)):
        val = helpers.try_to_access_dict(minidict[i], 'home', 'rinkSide')
        if val is not None:
            periods[helpers.try_to_access_dict(minidict[i], 'num')] = val
        else:
            periods[helpers.try_to_access_dict(minidict[i], 'num')] = 'N/A'
    while len(periods) < 3:
        periods[len(periods)] = 'N/A'

    return periods


def infer_zones_for_faceoffs(df, directions, xcol='X', ycol='Y', timecol='Time', focus_team=None, season=None):
    """
    Inferring zones for faceoffs from XY is hard--this method takes are of that.

    Basically, if you are in the first period and X is -69, you're in the offensive zone. But this flips if
    your team ID is smaller than the opp's ID

    This method notes several different zones:

    - OL (offensive zone, left)
    - OR (offensive zone, right)
    - NOL (neutral zone, near offensive blueline, left)
    - NOR (neutral zone, near offensive blueline, right)
    - NDL (neutral zone, near defensive blueline, left)
    - NDR (neutral zone, near defensive blueline, right)
    - DL (defensive zone, left)
    - DR (defensive zone, right)
    - N (center ice)

    :param df: dataframe with columns Game, specified xcol, and specified ycol
    :param directions: dataframe with columns Game, Period, and Direction ('left' or 'right')
    :param xcol: str, the column containing X coordinates in df
    :param ycol: str, the column containing Y coordinates in df
    :param timecol: str, the column containing the time in seconds.
    :param focus_team: int, str, or None. Directions are stored with home perspective. So specify focus team and will
        flip when focus_team is on the road. If None, does not do the extra home/road flip. Necessitates Season column
        in df.

    :return: dataframe with extra column FacLoc
    """

    # Center ice is easy
    df.loc[(df[xcol] == 0) & (df[ycol] == 0), 'FacLoc'] = 'N'

    # Infer periods
    df.loc[:, '_Period'] = df[timecol].apply(lambda x: x // 1200 + 1)

    # Join
    df2 = df.merge(directions.rename(columns={'Period': "_Period"}), how='left', on=['Game', '_Period'])

    # Flip for direction
    df2.loc[:, '_Mult'] = df2.Direction.apply(lambda x: 1 if x == 'right' else -1)
    df2.loc[:, '_Mult3'] = 1

    # Flip for home/road
    if focus_team is not None:
        focus_team = team_info.team_as_id(focus_team)

        season_dfs = []
        if 'Season' not in df2.columns:
            print('Need to have a Season column when invoking infer_zones_for_faceoffs with a focus_team')

        for season in df2.Season.value_counts().index:
            temp = df2.query('Season == {0:d}'.format(int(season)))
            team_sch = schedules.get_team_schedule(season, focus_team)
            team_sch = team_sch[['Game', 'Home', 'Road']] \
                .melt(id_vars='Game', var_name='_HR', value_name='Team') \
                .query('Team == {0:d}'.format(int(focus_team))) \
                .drop('Team', axis=1)
            team_sch.loc[:, '_Mult2'] = team_sch['_HR'].apply(lambda x: 1 if x == 'Home' else -1)
            team_sch = team_sch[['Game', '_Mult2']]

            df2 = df2.merge(team_sch, how='left', on='Game')
            df2.loc[:, '_Mult3'] = df2['_Mult'] * df2['_Mult2']
            df2 = df2.drop(['_Mult', '_Mult2'], axis=1)

    df2.loc[:, '_X'] = df2[xcol] * df2['_Mult3']
    df2.loc[:, '_Y'] = df2[ycol] * df2['_Mult3']

    df2.loc[(df2['_X'] == 69) & (df2['_Y'] == 22), 'FacLoc'] = 'OL'
    df2.loc[(df2['_X'] == 69) & (df2['_Y'] == -22), 'FacLoc'] = 'OR'
    df2.loc[(df2['_X'] == -69) & (df2['_Y'] == 22), 'FacLoc'] = 'DL'
    df2.loc[(df2['_X'] == -69) & (df2['_Y'] == -22), 'FacLoc'] = 'DR'

    df2.loc[(df2['_X'] == 20) & (df2['_Y'] == 22), 'FacLoc'] = 'NOL'
    df2.loc[(df2['_X'] == 20) & (df2['_Y'] == -22), 'FacLoc'] = 'NOR'
    df2.loc[(df2['_X'] == -20) & (df2['_Y'] == 22), 'FacLoc'] = 'NDL'
    df2.loc[(df2['_X'] == -20) & (df2['_Y'] == -22), 'FacLoc'] = 'NDR'

    df2.drop(['_X', '_Y', '_Period', '_Mult3', 'Direction'], axis=1, inplace=True)

    return df2


def generate_5v5_player_log(season):
    """
    Takes the play by play and adds player 5v5 info to the master player log file, noting TOI, CF, etc.
    This takes awhile because it has to calculate TOICOMP.
    :param season: int, the season
    :return: nothing
    """
    print('Generating player log for {0:d}'.format(season))

    to_concat = []

    # Recreate TOI60 file.
    _ = get_player_toion_toioff_file(season, force_create=True)

    for team in schedules.get_teams_in_season(season):
        try:
            goals = get_5v5_player_game_boxcars(season, team)  # G, A1, A2, SOG, iCF
            cfca = get_5v5_player_game_cfca(season, team)  # CFON, CAON, CFOFF, CAOFF
            gfga = get_5v5_player_game_gfga(season, team)  # GFON, GAON, GFOFF, GAOFF
            toi = get_5v5_player_game_toi(season, team)  # TOION and TOIOFF
            toicomp = get_5v5_player_game_toicomp(season, team)  # FQoC, F QoT, D QoC, D QoT, and respective Ns
            shifts = get_5v5_player_game_shift_startend(season, team)  # OZ, NZ, DZ, OTF-O, OTF-D, OTF-N

            temp = toi \
                .merge(cfca, how='left', on=['PlayerID', 'Game']) \
                .merge(gfga, how='left', on=['PlayerID', 'Game']) \
                .merge(toicomp.drop('Team', axis=1), how='left', on=['PlayerID', 'Game']) \
                .merge(goals, how='left', on=['PlayerID', 'Game']) \
                .merge(shifts, how='left', on=['PlayerID', 'Game']) \
                .assign(TeamID=team)

            to_concat.append(temp)
        except Exception as e:
            print('Issue with generating game-by-game for', season, team)
            print(e, e.args)

    print('Done generating for teams; aggregating')

    df = pd.concat(to_concat)
    for col in df.columns:
        df.loc[:, col] = pd.to_numeric(df[col])
    df = df[df.Game >= 20001]  # no preseason
    df = df[df.Game <= 30417]  # no ASG, WC, Olympics, etc
    for col in df:
        if df[col].isnull().sum() > 0:
            print('In player log, {0:s} has null values; filling with zeroes'.format(col))
            df.loc[:, col] = df[col].fillna(0)
    print('Done generating game-by-game')
    return df


def _get_5v5_player_game_fa(season, team, gc):
    """
    A helper method for get_5v5_player_game_cfca and _gfga.

    :param season: int, the season
    :param team: int, the team
    :param gc: use 'G' for goals and 'C' for Corsi.

    :return: dataframe
    """
    metrics = {'F': '{0:s}F'.format(gc),
               'A': '{0:s}A'.format(gc),
               'TeamF': 'Team{0:s}F'.format(gc),
               'TeamA': 'Team{0:s}A'.format(gc),
               'FON': '{0:s}FON'.format(gc),
               'AON': '{0:s}AON'.format(gc),
               'FOFF': '{0:s}FOFF'.format(gc),
               'AOFF': '{0:s}AOFF'.format(gc)}

    team = team_info.team_as_id(team)
    # TODO create generate methods. Get methods check if file exists and if not, create anew (or overwrite)
    pbp = filter_for_five_on_five(teams.get_team_pbp(season, team))
    if gc == 'G':
        pbp = filter_for_goals(pbp)
    elif gc == 'C':
        pbp = filter_for_corsi(pbp)

    pbp.loc[:, 'TeamEvent'] = pbp.Team.apply(lambda x: metrics['F'] if x == team else metrics['A'])

    teamtotals = pbp[['Game', 'TeamEvent']] \
        .assign(Count=1) \
        .groupby(['Game', 'TeamEvent']).count().reset_index() \
        .pivot_table(index='Game', columns='TeamEvent', values='Count').reset_index() \
        .rename(columns={metrics['F']: metrics['TeamF'], metrics['A']: metrics['TeamA']})

    toi = teams.get_team_toi(season, team)
    toi = toi[['Game', 'Time', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5']].drop_duplicates()
    indivtotals = pbp.merge(toi, how='left', on=['Game', 'Time'])
    indivtotals = indivtotals[['Game', 'TeamEvent', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5']] \
        .melt(id_vars=['Game', 'TeamEvent'], value_vars=['Team1', 'Team2', 'Team3', 'Team4', 'Team5'],
              var_name='Temp', value_name='PlayerID') \
        .drop('Temp', axis=1) \
        .assign(Count=1) \
        .groupby(['Game', 'TeamEvent', 'PlayerID']).count().reset_index() \
        .pivot_table(index=['Game', 'PlayerID'], columns='TeamEvent', values='Count').reset_index() \
        .rename(columns={metrics['F']: metrics['FON'], metrics['A']: metrics['AON']})

    df = indivtotals.merge(teamtotals, how='inner', on='Game')

    df = merge_onto_all_team_games_and_zero_fill(df, season, team)

    for col in [metrics['FON'], metrics['AON'], metrics['TeamF'], metrics['TeamA']]:
        if col not in df.columns:
            df.loc[:, col] = 0
        df.loc[:, col] = df[col].fillna(0)
    df.loc[:, metrics['FOFF']] = df[metrics['TeamF']] - df[metrics['FON']]
    df.loc[:, metrics['AOFF']] = df[metrics['TeamA']] - df[metrics['AON']]
    return df


def merge_onto_all_team_games_and_zero_fill(df, season, team):
    """
    A method that gets all team games from this season and left joins df onto it on game, then zero fills NAs.
    Makes sure you didn't miss any games and get NAs later.

    :param df: dataframe
    :param season: int, the season
    :param team: int or str, the team

    :return: dataframe
    """
    # Join onto schedule in case there were 0-0 games at 5v5
    sch = schedules.get_team_schedule(season, team)
    df = schedules.get_team_schedule(season, team)[['Game']].merge(df, how='left', on='Game')
    for col in df.columns:
        df.loc[:, col] = df[col].fillna(0)
    return df


def get_5v5_player_game_cfca(season, team):
    """
    Gets CFON, CAON, CFOFF, and CAOFF by game for given team in given season.

    :param season: int, the season
    :param team: int, team id

    :return: df with game, player, CFON, CAON, CFOFF, and CAOFF
    """
    return _get_5v5_player_game_fa(season, team, 'C')


def get_5v5_player_game_gfga(season, team):
    """
    Gets GFON, GAON, GFOFF, and GAOFF by game for given team in given season.

    :param season: int, the season
    :param team: int, team id

    :return: df with game, player, GFON, GAON, GFOFF, and GAOFF
    """
    return _get_5v5_player_game_fa(season, team, 'G')


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

    toi = parse_toi.get_parsed_toi(season, game)
    posdf = get_player_positions()

    fives = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "5")]
    cols_to_keep = ['Time'] + ['{0:s}{1:d}'.format(homeroad, i + 1) for i in range(5)]
    playersonice = fives[cols_to_keep] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .groupby('PlayerID').count().reset_index() \
        .rename(columns={'Time': 'Secs'}) \
        .merge(posdf, how='left', left_on='PlayerID', right_on='ID') \
        .drop('ID', axis=1) \
        .sort_values('Secs', ascending=False)
    if pos is not None:
        if pos == 'F':
            playersonice = playersonice.query('Pos != "D"')
        else:
            playersonice = playersonice.query('Pos == "{0:s}"'.format(pos))
    return playersonice


def get_line_combos(season, game, homeroad='H'):
    """
    Returns a df listing the 5v5 line combinations used in this game for specified team,
    and time they each played together

    :param season: int, the game
    :param game: int, the season
    :param homeroad: str, 'H' for home or 'R' for road

    :return: pandas dataframe with columns P1, P2, P3, Secs. May contain duplicates
    """

    toi = parse_toi.get_parsed_toi(season, game)
    pos = get_player_positions()

    fives = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "5")]
    cols_to_keep = ['Time'] + ['{0:s}{1:d}'.format(homeroad, i + 1) for i in range(5)]
    playersonice = fives[cols_to_keep] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .merge(pos, how='left', left_on='PlayerID', right_on='ID') \
        .query('Pos != "D"') \
        .drop({'Pos', 'ID'}, axis=1)
    wide = playersonice.merge(playersonice, how='inner', on='Time', suffixes=['1', '2']) \
        .merge(playersonice, how='inner', on='Time') \
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

    toi = parse_toi.get_parsed_toi(season, game)
    pos = get_player_positions()

    fives = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "5")]
    cols_to_keep = ['Time'] + ['{0:s}{1:d}'.format(homeroad, i + 1) for i in range(5)]
    playersonice = fives[cols_to_keep] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .merge(pos, how='left', left_on='PlayerID', right_on='ID') \
        .query('Pos == "D"') \
        .drop({'Pos', 'ID'}, axis=1)
    wide = playersonice.merge(playersonice, how='inner', on='Time', suffixes=['1', '2']) \
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
    toi = parse_toi.get_parsed_toi(season, game)
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


def filter_for_event_types(pbp, eventtype):
    """
    Filters given dataframe for event type(s) specified only.

    :param pbp: dataframe. Need a column titled Event
    :param eventtype: str or iterable of str, e.g. Goal, Shot, etc

    :return: dataframe, filtered
    """
    if isinstance(eventtype, str):
        return pbp[pbp.Event == eventtype]
    else:
        joindf = pd.DataFrame({'Event': list(set(eventtype))})
        return pbp.merge(joindf, how='inner', on='Event')


def filter_for_goals(pbp):
    """
    Filters given dataframe for goals only.

    :param pbp: dataframe. Need a column titled Event

    :return: dataframe. Only rows where Event == 'Goal'
    """
    return filter_for_event_types(pbp, 'Goal')


def filter_for_sog(pbp):
    """
    Filters given dataframe for SOG only.

    :param pbp: dataframe. Need a column titled Event

    :return: dataframe. Only rows where Event == 'Goal' or Event == 'Shot'
    """
    return filter_for_event_types(pbp, {'Goal', 'Shot'})


def filter_for_fenwick(pbp):
    """
    Filters given dataframe for SOG only.

    :param pbp: dataframe. Need a column titled Event

    :return: dataframe. Only rows where Event == 'Goal' or Event == 'Shot'
    """
    return filter_for_event_types(pbp, {'Goal', 'Shot', 'Missed Shot'})


def filter_for_five_on_five(df):
    """
    Filters given dataframe for 5v5 rows

    :param df: dataframe, columns HomeStrength + RoadStrength or TeamStrength + OppStrength

    :return: dataframe
    """
    colnames = set(df.columns)
    if 'HomeStrength' in colnames and 'RoadStrength' in colnames:
        fives = df[(df.HomeStrength == "5") & (df.RoadStrength == "5")]
    elif 'TeamStrength' in colnames and 'OppStrength' in colnames:
        fives = df[(df.TeamStrength == "5") & (df.OppStrength == "5")]
    else:
        fives = df
    return fives


def filter_for_corsi(pbp):
    """
    Filters given dataframe for goal, shot, miss, and block events

    :param pbp: a dataframe with column Event

    :return: pbp, filtered for corsi events
    """

    return filter_for_event_types(pbp, {'Goal', 'Shot', 'Missed Shot', 'Blocked Shot'})


def get_game_h2h_corsi(season, game):
    """
    This method gets H2H Corsi at 5v5 for the given game.

    :param season: int, the season
    :param game: int, the game

    :return: a df with [P1, P1Team, P2, P2Team, CF, CA, C+/-]. Entries will be duplicated, as with get_game_h2h_toi.
    """
    # TODO add strength arg
    toi = parse_toi.get_parsed_toi(season, game)
    pbp = parse_pbp.get_parsed_pbp(season, game)
    # toi.to_csv('/Users/muneebalam/Desktop/toi.csv')
    # pbp.to_csv('/Users/muneebalam/Desktop/pbp.csv')
    # pbp.loc[:, 'Event'] = pbp.Event.apply(lambda x: ss.convert_event(x))
    pbp = pbp[['Time', 'Event', 'Team']] \
        .merge(toi[['Time', 'R1', 'R2', 'R3', 'R4', 'R5', 'H1', 'H2', 'H3', 'H4', 'H5',
                    'HomeStrength', 'RoadStrength']], how='inner', on='Time')
    corsi = filter_for_five_on_five(filter_for_corsi(pbp)).drop(['HomeStrength', 'RoadStrength'], axis=1)

    hometeam = schedules.get_home_team(season, game)
    # Add HomeCorsi which will be 1 or -1. Need to separate out blocks because they're credited to defending team
    # Never mind, switched block attribution at time of parsing, so we're good now
    corsi.loc[:, 'HomeCorsi'] = corsi.Team.apply(lambda x: 1 if x == hometeam else -1)

    corsipm = corsi[['Time', 'HomeCorsi']]

    home = corsi[['Time', 'H1', 'H2', 'H3', 'H4', 'H5']] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .drop_duplicates()
    road = corsi[['Time', 'R1', 'R2', 'R3', 'R4', 'R5']] \
        .melt(id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1)

    hh = home.merge(home.drop_duplicates(), how='inner', on='Time', suffixes=['1', '2']).assign(Team1='H', Team2='H')
    hr = home.merge(road.drop_duplicates(), how='inner', on='Time', suffixes=['1', '2']).assign(Team1='H', Team2='R')
    rh = road.merge(home.drop_duplicates(), how='inner', on='Time', suffixes=['1', '2']).assign(Team1='R', Team2='H')
    rr = road.merge(road.drop_duplicates(), how='inner', on='Time', suffixes=['1', '2']).assign(Team1='R', Team2='R')

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
    playersonice = players.get_player_ids_file()[['ID', 'Name']]

    newdf = pd.DataFrame(index=df.index)
    for col in colnames:
        if col in columns:
            newdf = newdf.merge(players, how='left', left_on=col, right_on='ID') \
                .drop([col, 'ID'], axis=1) \
                .rename(columns={'ID': col})
        else:
            newdf.loc[:, col] = df[col]

    return newdf


if __name__ == '__main__':
    for season in range(2016, 2018):
        get_5v5_player_log(season, True)
