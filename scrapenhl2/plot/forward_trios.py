"""
This module contains methods for creating a scatterplot of team forward line shot rates.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mplc

import scrapenhl2.plot.visualization_helper as vhelper
from scrapenhl2.scrape import schedules, team_info, teams, players
import scrapenhl2.scrape.general_helpers as helper
from scrapenhl2.manipulate import manipulate as manip, combos

def team_fline_shot_rates_scatter(team, min_line_toi=50, **kwargs):
    """
    Creates a scatterplot of team forward line shot attempr rates.

    :param team: int or str, team
    :param min_line_toi: int, number of minutes for pair to qualify
    :param kwargs: Use season- or date-range-related kwargs only.

    :return:
    """

    kwargs['team'] = team

    startdate, enddate = vhelper.get_startdate_enddate_from_kwargs(**kwargs)
    rates = get_fline_shot_rates(team, startdate, enddate)
    lines = drop_duplicate_lines(rates)
    xy = _add_xy_names_for_fline_graph(lines)

    xy = _get_colors_markers_for_fline_scatter(xy)

    # Remove players who didn't have at least one line combination above minimum
    # Remove total TOI rows first, then filter
    # Get indiv toi by finding index of max TOI of each group. Then anti-join lines onto indiv toi
    indivtoi = xy.ix[xy.groupby(['Name', 'PlayerID'], as_index=False)['TOI'].idxmax()] \
        [['Name', 'PlayerID', 'TOI', 'X', 'Y', 'Color', 'Marker']] \
        .sort_values('TOI', ascending=False)
    xy = helper.anti_join(xy.query('TOI >= {0:d}'.format(60 * min_line_toi)),
                          indivtoi[['Name', 'PlayerID', 'TOI']], on=['Name', 'PlayerID', 'TOI'])

    # Now get sizes. Scaling is too poor if I do it earlier
    xy = _get_point_sizes_for_fline_scatter(xy)

    # Plot individuals
    # Ordinarily would filter for players with a qualifying line combo again
    # But this would eliminate some fourth liners who are lineup constants
    # Instead, make sure anybody with at least as much TOI as anybody on a qualifying line is in
    mintoi = indivtoi[['PlayerID', 'TOI']] \
        .merge(pd.DataFrame({'PlayerID': xy.PlayerID.unique()}), how='inner', on='PlayerID') \
        .TOI.min()
    indivtoi = indivtoi.query('TOI >= {0:d}'.format(int(mintoi)))

    fig = plt.figure(figsize=[8, 6])
    ax = plt.gca()
    for i, name, pid, toi, x, y, color, marker in indivtoi.itertuples():
        # Size gets too crazy, so fix it
        ax.scatter([x], [y], marker=marker, s=200, c=color, label=helper.get_lastname(name))

    # Now plot lines
    for name in xy.Name.unique():
        temp = xy.query('Name == "{0:s}"'.format(name)).sort_values('TOI', ascending=False)
        if len(temp) == 0:
            continue
        ax.scatter(temp.X.values, temp.Y.values, marker=temp.Marker.values[0], s=temp.Size.values, c=temp.Color.values)

    ax.set_xlabel('CF60')
    ax.set_ylabel('CA60')
    num_players = len(xy.Name.unique())
    plt.legend(loc='upper center', fontsize=6, ncol=num_players//3+1)
    vhelper.add_good_bad_fast_slow()
    vhelper.add_cfpct_ref_lines_to_plot(ax)

    ax.set_title(', '.join(vhelper.generic_5v5_log_graph_title('F line shot rates', **kwargs)))

    return vhelper.savefilehelper(**kwargs)


def _get_colors_markers_for_fline_scatter(df):
    """
    A helper method that scales scatterpoint alphas corresponding to TOI column. The largest point gets an alpha of 0.9;
    others get smaller linearly. Follows current matplotlib color cycle, turning RGB into RGBA.
    Top 3 forwards get a star marker, next six get a plus, rest get up triangles

    :param df: dataframe with TOI column

    :return: df with an extra column Color.
    """

    largest = df.TOI.max()

    color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']

    def get_adjusted_color(base, largesttoi, thistoi):
        newcolor = mplc.to_rgba(base, alpha=thistoi / largesttoi * 0.9)
        return newcolor

    toisums = df[['PlayerID', 'Name', 'TOI']] \
        .groupby(['PlayerID', 'Name'], as_index=False) \
        .sum() \
        .sort_values('TOI', ascending=False) \
        .drop('TOI', axis=1)
    markers = ['*'] * 3 + ['P'] * 3 + ['^'] * 3 + ['v'] * 30  # very large
    toisums = toisums.assign(Marker=markers[:len(toisums)])

    dflst = []
    for i, name in enumerate(toisums.Name):
        if i < 9:
            j = i % 3
        else:
            j = i - 9
        color = vhelper.hex_to_rgb(color_cycle[j], maxval=1)
        temp = df.query('Name == "{0:s}"'.format(name))
        temp.loc[:, 'Color'] = temp.TOI.apply(lambda x: get_adjusted_color(color, largest, x))
        dflst.append(temp)

    df2 = pd.concat(dflst).merge(toisums, how='left', on=['PlayerID', 'Name'])
    return df2


def _get_point_sizes_for_fline_scatter(df):
    """
    A helper method that scales scatterpoint sizes corresponding to TOI column. The largest point gets a size of 200;
    others get smaller linearly.

    :param df: dataframe with TOI column

    :return: df with an extra column Size that can be used in matplotlib as the kwarg 's'
    """

    largest = df.TOI.max()
    df.loc[:, 'Size'] = (df.TOI / largest) * 200
    return df


def _add_xy_names_for_fline_graph(df, delta=0.75):
    """
    X is CF60 and Y is CA60. Pushes PlayerID1 a little to the left, playerID2 a little up, and PlayerID3 right.
    Also adds player names.

    :param df: dataframe with CF60 and CA60. This df will be wide.
    :param delta: amount to move by, in data coordinates

    :return: dataframe with X and Y and names added on, melted version of original df
    """
    df = df.assign(LineIndex=1)
    df.loc[:, 'LineIndex'] = df.LineIndex.cumsum()
    melted = helper.melt_helper(df[['CF60', 'CA60', 'TOI', 'PlayerID1', 'PlayerID2', 'PlayerID3', 'LineIndex']],
                                id_vars=['CF60', 'CA60', 'TOI', 'LineIndex'],
                                var_name='P1P2P3', value_name='PlayerID')
    melted.loc[:, 'Name'] = melted.PlayerID.apply(lambda x: players.player_as_str(x))

    # Extract singles, pairs, and triples
    temp = melted[['TOI', 'LineIndex', 'PlayerID']] \
        .drop_duplicates() \
        .rename(columns={'PlayerID': 'Count'}) \
        .groupby(['TOI', 'LineIndex'], as_index=False) \
        .count() \
        .merge(melted, how='left', on=['TOI', 'LineIndex'])
    singles = temp.query('Count == 1').drop('Count', axis=1) \
        .assign(P1P2P3='PlayerID1').drop_duplicates()
    #pairs = temp.query('Count == 2').drop('Count', axis=1) \
    #    .assign(P1P2P3='PlayerID1').drop_duplicates(subset=)
    triples = temp.query('Count == 3').drop('Count', axis=1)

    # For triples, do the shift. For singles, no shift. For pairs, shift left and right only.
    triples.loc[:, 'DeltaX'] = triples.P1P2P3.apply(lambda x: {'PlayerID1': -1 * delta,
                                                             'PlayerID2': 0,
                                                             'PlayerID3': delta}[x])
    triples.loc[:, 'DeltaY'] = triples.P1P2P3.apply(lambda x: {'PlayerID1': 0,
                                                             'PlayerID2': delta,
                                                             'PlayerID3': 0}[x])
    melted = pd.concat([singles, triples]).fillna(0)

    melted.loc[:, 'X'] = melted.CF60 + melted.DeltaX
    melted.loc[:, 'Y'] = melted.CA60 + melted.DeltaY
    melted = melted.drop({'DeltaX', 'DeltaY', 'LineIndex'}, axis=1)

    return melted


def drop_duplicate_lines(rates):
    """
    The shot rates dataframe has duplicates--e.g. one row is Ovechkin-Backstrom-Oshie, in another
    Oshie-Ovechkin-Backstrom. This method will select only one.

    For now, it arranges by PlayerID, but in the future, it will use the following rules:

    - If there is exactly one center
        - If you have a L and R as well, pick the L-C-R line
        - If the wings are different handedness, pick lefty-C-righty
        - Otherwise, the left wing is the one with the smaller playerID
    - If there are multiple centers
        - Pick the one with most draws taken as the true center
        - Select a remaining wing if possible, and if both remaining players have the same position,
        attribute based on handedness, and if that doesn't work, arrange by PlayerID

    :param rates: dataframe as created by get_fline_shot_rates

    :return: dataframe, rates with half of rows dropped
    """

    # Melt and arrange, and pick first
    lines = rates[['PlayerID1', 'PlayerID2', 'PlayerID3']].assign(LineIndex=1)
    lines.loc[:, 'LineIndex'] = lines.LineIndex.cumsum()
    melted = helper.melt_helper(lines, id_vars='LineIndex', var_name='P1P2P3', value_name='PlayerID')

    grouped = melted.sort_values(['LineIndex', 'PlayerID'])\
        .drop('P1P2P3', axis=1) \
        .groupby('LineIndex', as_index=False)

    firsts = grouped.first().rename(columns={'PlayerID': 'PlayerID1'})
    middles = grouped.median().rename(columns={'PlayerID': 'PlayerID2'})
    lasts = grouped.last().rename(columns={'PlayerID': 'PlayerID3'})

    joined = lines[['LineIndex']] \
        .merge(firsts, how='left', on='LineIndex') \
        .merge(middles, how='left', on='LineIndex') \
        .merge(lasts, how='left', on='LineIndex') \
        .drop('LineIndex', axis=1) \
        .drop_duplicates()

    # Inner join back on
    df = rates.merge(joined, how='inner', on=['PlayerID1', 'PlayerID2', 'PlayerID3'])

    return df


def get_fline_shot_rates(team, startdate, enddate):
    """
    Gets CF/60 and CA/60 by defenseman duo (5v5 only) for this team between given range of dates

    :param team: int or str, team
    :param startdate: str, start date
    :param enddate: str, end date (inclusive)

    :return: dataframe with PlayerID1, PlayerID2, CF, CA, TOI (in secs), CF/60 and CA/60
    """
    # TODO this method is so slow

    startseason, endseason = [helper.infer_season_from_date(x) for x in (startdate, enddate)]

    dflst = []
    for season in range(startseason, endseason+1):
        games_played = schedules.get_team_games(season, team, startdate, enddate)
        games_played = [g for g in games_played if 20001 <= g <= 30417]

        toi = combos.get_team_combo_toi(season, team, games_played, n_players=3) \
            .rename(columns={'Secs': 'TOI'})

        cfca = combos.get_team_combo_corsi(season, team, games_played, n_players=3)

        joined = toi.merge(cfca, how='outer', on=['PlayerID1', 'PlayerID2', 'PlayerID3']) \
            .assign(Season=season)
        dflst.append(joined)

    df = pd.concat(dflst) \
        .groupby(['PlayerID1', 'PlayerID2', 'PlayerID3'], as_index=False).sum()
    df.loc[:, 'CF60'] = df.CF * 3600 / df.TOI
    df.loc[:, 'CA60'] = df.CA * 3600 / df.TOI

    forwards = players.get_player_ids_file().query('Pos != "D"')[['ID']]
    df = df.merge(forwards.rename(columns={'ID': 'PlayerID1'}), how='inner', on='PlayerID1') \
        .merge(forwards.rename(columns={'ID': 'PlayerID2'}), how='inner', on='PlayerID2') \
        .merge(forwards.rename(columns={'ID': 'PlayerID3'}), how='inner', on='PlayerID3')

    return df
