"""
This module contains methods for generating H2H data for games
"""
import pandas as pd

from scrapenhl2.manipulate import manipulate as manip, add_onice_players as onice
from scrapenhl2.scrape import general_helpers as helpers, parse_toi, parse_pbp, schedules, team_info, teams


def get_game_combo_toi(season, game, player_n=2, *hrcodes):
    """
    This method gets H2H TOI at 5v5 for the given game.

    :param season: int, the season
    :param game: int, the game
    :param player_n: int. E.g. 1 gives you a list of players and TOI, 2 gives you h2h, 3 gives you groups of 3, etc.
    :param hrcodes: to limit exploding joins, specify strings containing 'H' and 'R' and 'A', each of length player_n
        For example, if player_n=3, specify 'HHH' to only get home team player combos.
        If this is left unspecified, will do all combos, which can be problematic when player_n > 3.
        'R' for road, 'H' for home, 'A' for all (both)

    :return: a df with [P1, P1Team, P2, P2Team, TOI, etc]. Entries will be duplicated.
    """

    if len(hrcodes) == 0:
        hrcodes = ['A'*player_n]
    for hrcode in hrcodes:
        assert len(hrcode) == player_n

    home, road = parse_toi.get_melted_home_road_5v5_toi(season, game)

    return _combo_secs_from_hrcodes(home, road, *hrcodes)


def get_game_combo_corsi(season, game, player_n=2, cfca=None, *hrcodes):
    """
    This method gets H2H Corsi at 5v5 for the given game.

    :param season: int, the season
    :param game: int, the game
    :param player_n: int. E.g. 1 gives you a list of players and TOI, 2 gives you h2h, 3 gives you groups of 3, etc.
    :param cfca: str, or None. If you specify 'cf', returns CF only. For CA, use 'ca'. None returns CF - CA.
    :param hrcodes: to limit exploding joins, specify strings containing 'H' and 'R' and 'A', each of length player_n
        For example, if player_n=3, specify 'HHH' to only get home team player combos.
        If this is left unspecified, will do all combos, which can be problematic when player_n > 3.
        'R' for road, 'H' for home, 'A' for all (both)

    :return: a df with [P1, P1Team, P2, P2Team, TOI, etc]. Entries will be duplicated.
    """

    if len(hrcodes) == 0:
        hrcodes = ['A'*player_n]
    for hrcode in hrcodes:
        assert len(hrcode) == player_n

    corsipm = parse_pbp.get_5v5_corsi_pm(season, game)
    home, road = parse_toi.get_melted_home_road_5v5_toi(season, game)

    return _combo_corsi_from_hrcodes(home, road, corsipm, cfca, *hrcodes)


def _combo_corsi_from_hrcodes(homedf=None, roaddf=None, corsidf=None, cfca=None, *hrcodes):
    """
    Joins the homedf and roaddf as specified by hrcodes.

    :param homedf: home team df (e.g. for TOI)
    :param roaddf: road team df (e.g. for TOI)
    :param corsidf: a dataframe with Time and HomeCorsi (1 or -1), one row per event
    :param hrcodes: to limit exploding joins, specify strings containing 'H' and 'R' and 'A', each of length player_n
        For example, if player_n=3, specify 'HHH' to only get home team player combos.
        If this is left unspecified, will do all combos, which can be problematic when player_n > 3.
        'R' for road, 'H' for home, 'A' for all (both)

    :return: joined df, grouped and summed by player combos
    """

    alldf = pd.concat([homedf, roaddf])

    dflst = []
    for hrcode in hrcodes:
        dfs_to_join = []
        for i in range(len(hrcode)):
            if hrcode[i].upper() == 'H':
                dfs_to_join.append(homedf)
            elif hrcode[i].lower() == 'R':
                dfs_to_join.append(roaddf)
            else:
                dfs_to_join.append(alldf)
        gamedf = None
        for i, df in enumerate(dfs_to_join):
            if gamedf is None:
                gamedf = df
            else:
                # Drop duplicates so, e.g. if you have 2 shots in a second, final df registers that twice, not 4x
                gamedf = gamedf.merge(df.drop_duplicates(), how='inner', on='Time', suffixes=['', str(i + 1)])

        gamedf = gamedf.rename(columns={'PlayerID': 'PlayerID1', 'Team': 'Team1'}) \
            .merge(corsidf, how='inner', on='Time') \
            .drop('Time', axis=1)

        if cfca is None:
            gamedf.loc[gamedf.Team1 == 'R', 'HomeCorsi'] = gamedf.loc[gamedf.Team1 == 'R', 'HomeCorsi'] * -1

        gamedf = gamedf.groupby([col for col in gamedf.columns if col != 'HomeCorsi'], as_index=False).sum()

        # One last to-do: make sure I have all possible pairs of players covered
        combocols = tuple([('PlayerID' + str(x), 'Team' + str(x)) for x in range(1, len(hrcodes[0]) + 1)])
        allcombos = manip.convert_to_all_combos(gamedf, 0, *combocols)
        dflst.append(allcombos)

    return pd.concat(dflst)


def _combo_secs_from_hrcodes(homedf=None, roaddf=None, *hrcodes):
    """
    Joins the homedf and roaddf as specified by hrcodes.

    :param homedf: home team df (e.g. for TOI)
    :param roaddf: road team df (e.g. for TOI)
    :param hrcodes: to limit exploding joins, specify strings containing 'H' and 'R' and 'A', each of length player_n
        For example, if player_n=3, specify 'HHH' to only get home team player combos.
        If this is left unspecified, will do all combos, which can be problematic when player_n > 3.
        'R' for road, 'H' for home, 'A' for all (both)

    :return: joined df, grouped and summed
    """

    alldf = pd.concat([homedf, roaddf])

    dflst = []
    for hrcode in hrcodes:
        dfs_to_join = []
        for i in range(len(hrcode)):
            if hrcode[i].upper() == 'H':
                dfs_to_join.append(homedf)
            elif hrcode[i].lower() == 'R':
                dfs_to_join.append(roaddf)
            else:
                dfs_to_join.append(alldf)
        gamedf = None
        for i, df in enumerate(dfs_to_join):
            if gamedf is None:
                gamedf = df
            else:
                gamedf = gamedf.merge(df, how='inner', on='Time', suffixes=['', str(i + 1)])

        gamedf = gamedf.rename(columns={'PlayerID': 'PlayerID1', 'Team': 'Team1'}) \
            .assign(Secs=1) \
            .drop('Time', axis=1)
        gamedf = gamedf.groupby([col for col in gamedf.columns if col != 'Secs'], as_index=False).count()

        # One last to-do: make sure I have all possible pairs of players covered
        combocols = tuple([('PlayerID' + str(x), 'Team' + str(x)) for x in range(1, len(hrcodes[0]) + 1)])
        allcombos = manip.convert_to_all_combos(gamedf, 0, *combocols)

        allcombos.loc[:, 'Min'] = allcombos.Secs / 60
        dflst.append(allcombos)

    return pd.concat(dflst)


def get_team_combo_toi(season, team, games, n_players=2):
    """
    Gets 5v5 combo TOI for team for specified games

    :param season: int, the season
    :param team: int or str, team
    :param games: int or iterable of int, games
    :param n_players: int. E.g. 1 gives you player TOI, 2 gives you 2-player group TOI, 3 makes 3-player groups, etc

    :return: dataframe
    """

    if helpers.check_number(games):
        games = [games]

    teamid = team_info.team_as_id(team)
    toi = teams.get_team_toi(season, team) \
        .merge(pd.DataFrame({'Game': games}), how='inner', on='Game') \
        .pipe(manip.filter_for_five_on_five) \
        [['Game', 'Time', 'Team1', 'Team2', 'Team3', 'Team4', 'Team5']] \
        .pipe(helpers.melt_helper, id_vars=['Game', 'Time'], var_name='P', value_name='PlayerID') \
        .drop('P', axis=1)
    toi2 = None
    for i in range(n_players):
        toitemp = toi.rename(columns={'PlayerID': 'PlayerID' + str(i+1)})
        if toi2 is None:
            toi2 = toitemp
        else:
            toi2 = toi2.merge(toitemp, how='inner', on=['Game', 'Time'])

    # Group by players and count
    groupcols = ['PlayerID' + str(i+1) for i in range(n_players)]
    grouped = toi2.drop('Game', axis=1) \
        .groupby(groupcols, as_index=False) \
        .count() \
        .rename(columns={'Time': 'Secs'})

    # Convert to all columns
    allcombos = manip.convert_to_all_combos(grouped, 0, *groupcols)
    return allcombos


def get_team_combo_corsi(season, team, games, n_players=2):
    """
    Gets combo Corsi for team for specified games

    :param season: int, the season
    :param team: int or str, team
    :param games: int or iterable of int, games
    :param n_players: int. E.g. 1 gives you player TOI, 2 gives you 2-player group TOI, 3 makes 3-player groups, etc

    :return: dataframe
    """

    if helpers.check_number(games):
        games = [games]

    teamid = team_info.team_as_id(team)
    corsi = teams.get_team_pbp(season, team)
    corsi = corsi.assign(_Secs=corsi.Time) \
        .merge(pd.DataFrame({'Game': games}), how='inner', on='Game') \
        .pipe(manip.filter_for_five_on_five) \
        .pipe(manip.filter_for_corsi) \
        [['Game', 'Time', 'Team', '_Secs']] \
        .pipe(onice.add_onice_players_to_df, focus_team=team, season=season, gamecol='Game')
    cols_to_drop = ['Opp{0:d}'.format(i) for i in range(1, 7)] + ['{0:s}6'.format(team_info.team_as_str(team))]
    corsi = corsi.drop(cols_to_drop, axis=1) \
        .pipe(helpers.melt_helper, id_vars=['Game', 'Time', 'Team'], var_name='P', value_name='PlayerID') \
        .drop('P', axis=1)
    corsi2 = None
    for i in range(n_players):
        corsitemp = corsi.rename(columns={'PlayerID': 'PlayerID' + str(i+1)})
        if corsi2 is None:
            corsi2 = corsitemp
        else:
            corsi2 = corsi2.merge(corsitemp, how='inner', on=['Game', 'Time', 'Team'])

    # Assign CF and CA
    teamid = team_info.team_as_id(team)
    corsi2.loc[:, 'CF'] = corsi2.Team.apply(lambda x: 1 if x == teamid else 0)
    corsi2.loc[:, 'CA'] = corsi2.Team.apply(lambda x: 0 if x == teamid else 1)
    corsi2 = corsi2.drop({'Game', 'Time', 'Team'}, axis=1)

    # Group by players and count
    groupcols = ['PlayerID' + str(i+1) for i in range(n_players)]
    grouped = corsi2 \
        .groupby(groupcols, as_index=False) \
        .sum() \
        .rename(columns={'Time': 'Secs'})

    # Convert to all columns
    allcombos = manip.convert_to_all_combos(grouped, 0, *groupcols)
    return allcombos