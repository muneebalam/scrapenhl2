"""
This module contains methods for generating H2H data for games
"""
import pandas as pd

from scrapenhl2.manipulate import manipulate as manip
from scrapenhl2.scrape import general_helpers as helpers, parse_toi, parse_pbp, schedules


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

    toi = parse_toi.get_parsed_toi(season, game)
    fives = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "5")]
    home = helpers.melt_helper(fives[['Time', 'H1', 'H2', 'H3', 'H4', 'H5']],
                               id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .assign(Team='H')
    road = helpers.melt_helper(fives[['Time', 'R1', 'R2', 'R3', 'R4', 'R5']],
                               id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .assign(Team='R')

    return _combo_secs_from_hrcodes(home, road, *hrcodes)


def get_game_h2h_corsi(season, game, player_n=2, cfca=None, *hrcodes):
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

    toi = parse_toi.get_parsed_toi(season, game)
    pbp = parse_pbp.get_parsed_pbp(season, game)

    pbp = pbp[['Time', 'Event', 'Team']] \
        .merge(toi[['Time', 'R1', 'R2', 'R3', 'R4', 'R5', 'H1', 'H2', 'H3', 'H4', 'H5',
                    'HomeStrength', 'RoadStrength']], how='inner', on='Time')
    corsi = manip.filter_for_five_on_five(manip.filter_for_corsi(pbp)) \
        .drop(['HomeStrength', 'RoadStrength'], axis=1)

    hometeam = schedules.get_home_team(season, game)

    if cfca is None:
        corsi.loc[:, 'HomeCorsi'] = corsi.Team.apply(lambda x: 1 if x == hometeam else -1)
    elif cfca == 'cf':
        corsi.loc[:, 'HomeCorsi'] = corsi.Team.apply(lambda x: 1 if x == hometeam else 0)
    elif cfca == 'ca':
        corsi.loc[:, 'HomeCorsi'] = corsi.Team.apply(lambda x: 0 if x == hometeam else 1)

    corsipm = corsi[['Time', 'HomeCorsi']]

    home = helpers.melt_helper(corsi[['Time', 'H1', 'H2', 'H3', 'H4', 'H5']],
                               id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1) \
        .drop_duplicates()
    road = helpers.melt_helper(corsi[['Time', 'R1', 'R2', 'R3', 'R4', 'R5']],
                               id_vars='Time', var_name='P', value_name='PlayerID') \
        .drop('P', axis=1)

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
        allcombos.loc[:, 'Min'] = allcombos.Secs / 60
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
