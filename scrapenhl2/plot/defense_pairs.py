"""
This module contains methods for creating a scatterplot of team defense pair shot rates.
"""

import pandas as pd
import matplotlib.pyplot as plt

import scrapenhl2.plot.visualization_helper as vhelper
from scrapenhl2.scrape import schedules, team_info, teams, players
import scrapenhl2.scrape.general_helpers as helper
import scrapenhl2.manipulate.manipulate as manip

def team_dpair_shot_rates_scatter(team, min_pair_toi=50, **kwargs):
    """

    :param team:
    :param min_pair_toi: int, number of minutes for pair to qualify
    :param kwargs:
    :return:
    """

    kwargs['team'] = team
    startdate, enddate = vhelper.get_startdate_enddate_from_kwargs(**kwargs)
    rates = get_dpair_shot_rates(team, startdate, enddate)

    pairs = drop_duplicate_pairs(rates).query('TOI >= {0:d}'.format(60 * min_pair_toi))

    xy = _add_xy_names_for_dpair_graph(pairs)

    fig = plt.figure(figsize=[8, 6])
    ax = plt.gca()

    for name in xy.Name.unique():
        temp = xy.query('Name == "{0:s}"'.format(name))
        # TODO alpha or size based on TOI?
        ax.scatter(temp.X.values, temp.Y.values, label=name, s=200, alpha=0.5)

    ax.set_xlabel('CF60')
    ax.set_ylabel('CA60')
    plt.legend(loc=2, fontsize=10, bbox_to_anchor=(1, 1))
    vhelper.add_cfpct_ref_lines_to_plot(ax)

    return vhelper.savefilehelper(**kwargs)


def _add_xy_names_for_dpair_graph(df, delta=0.25):
    """
    X is CF60 and Y is CA60. Pushes PlayerID1 a little to the left and PlayerID2 a little to the right in X. Also
    adds player names.

    :param df: dataframe with CF60 and CA60. This df will be wide.
    :param delta: amount to move by, in data coordinates

    :return: dataframe with X and Y and names added on, melted version of original df
    """

    melted = helper.melt_helper(df[['PlayerID1', 'PlayerID2', 'CF60', 'CA60', 'TOI']],
                                id_vars=['CF60', 'CA60', 'TOI'], var_name='P1P2', value_name='PlayerID')

    melted.loc[:, 'Name'] = melted.PlayerID.apply(lambda x: players.player_as_str(x))
    melted.loc[melted.P1P2 == 'PlayerID1', 'X'] = melted.loc[melted.P1P2 == 'PlayerID1', 'CF60'] - delta
    melted.loc[melted.P1P2 == 'PlayerID2', 'X'] = melted.loc[melted.P1P2 == 'PlayerID2', 'CF60'] + delta
    melted.loc[:, 'Y'] = melted.CA60

    return melted


def drop_duplicate_pairs(rates):
    """
    The shot rates dataframe has duplicates--e.g. in one row Orlov is PlayerID1 and Niskanen PlayerID2, but in
    another Niskanen is PlayerID1 and Orlov is playerID2. This method will select only one, using the following rules:

    - For mixed-hand pairs, pick the one where P1 is the lefty and P2 is the righty
    - For other pairs, arrange by PlayerID. The one with the smaller ID is P1 and the larger, P2.

    :param rates: dataframe as created by get_dpair_shot_rates

    :return: dataframe, rates with half of rows dropped
    """

    handedness = players.get_player_ids_file().query('Pos == "D"')[['ID', 'Hand']]
    rates = rates.merge(handedness.rename(columns={'ID': 'PlayerID1', 'Hand': 'Hand1'})) \
        .merge(handedness.rename(columns={'ID': 'PlayerID2', 'Hand': 'Hand2'}))

    rates = rates[((rates.Hand1 == "R") & (rates.Hand2 == "L")) == False]

    lr_pairs = rates.query('Hand1 == "L" & Hand2 == "R"')  # Will keep these
    ll_rr_pairs = rates[((rates.Hand1 == "L") & (rates.Hand2 == "R")) == False]

    # Melt and arrange, and pick first
    ll_rr_pairs = ll_rr_pairs[['PlayerID1', 'PlayerID2']].assign(PairIndex=1)
    ll_rr_pairs.loc[:, 'PairIndex'] = ll_rr_pairs.PairIndex.cumsum()
    melted = helper.melt_helper(ll_rr_pairs, id_vars='PairIndex', var_name='P1P2', value_name='PlayerID')

    firsts = melted.sort_values(['PairIndex', 'PlayerID']) \
        .groupby('PairIndex', as_index=False) \
        .first() \
        .drop('P1P2', axis=1) \
        .rename(columns={'PlayerID': 'PlayerID1'})
    lasts = melted.sort_values(['PairIndex', 'PlayerID']) \
        .groupby('PairIndex', as_index=False) \
        .last() \
        .drop('P1P2', axis=1) \
        .rename(columns={'PlayerID': 'PlayerID2'})

    joined = firsts.merge(lasts, how='outer', on='PairIndex').drop('PairIndex', axis=1)

    # Inner join back on
    df = pd.concat([lr_pairs,
                    rates.merge(joined, how='inner', on=['PlayerID1', 'PlayerID2'])]) \
        .drop({'Hand1', 'Hand2'}, axis=1)

    return df


def get_dpair_shot_rates(team, startdate, enddate):
    """
    Gets CF/60 and CA/60 by defenseman duo (5v5 only) for this team between given range of dates

    :param team: int or str, team
    :param startdate: str, start date
    :param enddate: str, end date (inclusive)

    :return: dataframe with PlayerID1, PlayerID2, CF, CA, TOI (in secs), CF/60 and CA/60
    """
    startseason, endseason = [helper.infer_season_from_date(x) for x in (startdate, enddate)]

    dflst = []
    for season in range(startseason, endseason+1):
        games_played = schedules.get_team_games(season, team, startdate, enddate)
        games_played = [g for g in games_played if g >= 20001 and g <= 30417]
        toi = manip.get_game_h2h_toi(season, games_played).rename(columns={'Secs': 'TOI'})
        cf = manip.get_game_h2h_corsi(season, games_played, 'cf').rename(columns={'HomeCorsi': 'CF'})
        ca = manip.get_game_h2h_corsi(season, games_played, 'ca').rename(columns={'HomeCorsi': 'CA'})

        # TOI, CF, and CA have columns designating which team--H or R
        # Use schedule to find appropriate ones to filter for
        sch = schedules.get_team_schedule(season, team, startdate, enddate)
        sch = helper.melt_helper(sch[['Game', 'Home', 'Road']],
                                 id_vars='Game', var_name='HR', value_name='Team')
        sch = sch.query('Team == {0:d}'.format(int(team_info.team_as_id(team))))
        sch.loc[:, 'HR'] = sch.HR.apply(lambda x: x[0])
        sch = sch.assign(Team1=sch.HR, Team2=sch.HR).drop({'Team', 'HR'}, axis=1)

        toi = toi.merge(sch, how='inner', on=['Game', 'Team1', 'Team2'])
        cf = cf.merge(sch, how='inner', on=['Game', 'Team1', 'Team2'])
        ca = ca.merge(sch, how='inner', on=['Game', 'Team1', 'Team2'])

        # CF and CA from home perspective, so switch if necessary
        cfca = cf.merge(ca, how='outer', on=['Game', 'PlayerID1', 'PlayerID2', 'Team1', 'Team2'])
        cfca.loc[:, 'tempcf'] = cfca.CF
        cfca.loc[:, 'tempca'] = cfca.CA
        cfca.loc[cf.Team1 == 'R', 'CF'] = cfca[cfca.Team1 == 'R'].tempca
        cfca.loc[ca.Team1 == 'R', 'CA'] = cfca[cfca.Team1 == 'R'].tempcf

        cfca = cfca.drop({'Team1', 'Team2', 'tempcf', 'tempca'}, axis=1)
        toi = toi.drop({'Team1', 'Team2', 'Min'}, axis=1)

        joined = toi.merge(cfca, how='outer', on=['PlayerID1', 'PlayerID2', 'Game']) \
            .assign(Season=season)
        dflst.append(joined)

    df = pd.concat(dflst) \
        .groupby(['PlayerID1', 'PlayerID2'], as_index=False).sum()
    df.loc[:, 'CF60'] = df.CF * 3600 / df.TOI
    df.loc[:, 'CA60'] = df.CA * 3600 / df.TOI

    defensemen = players.get_player_ids_file().query('Pos == "D"')[['ID']]
    df = df.merge(defensemen.rename(columns={'ID': 'PlayerID1'}), how='inner', on='PlayerID1') \
        .merge(defensemen.rename(columns={'ID': 'PlayerID2'}), how='inner', on='PlayerID2')

    return df
