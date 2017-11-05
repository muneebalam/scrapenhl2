"""
This module creates rolling CF% and GF% charts
"""

import matplotlib.pyplot as plt
import numpy as np  # standard scientific python stack
import pandas as pd  # standard scientific python stack

import scrapenhl2.manipulate.manipulate as manip
from scrapenhl2.scrape import players
from scrapenhl2.scrape import schedules
from scrapenhl2.plot import visualization_helper as vhelper

def rolling_player_gf(player, **kwargs):
    """
    Creates a graph with GF% and GF% off. Defaults to roll_len of 40.

    :param player: str or int, player to generate for
    :param kwargs: other filters. See scrapenhl2.plot.visualization_helper.get_and_filter_5v5_log for more information.

    :return: nothing, or figure
    """
    if 'roll_len' not in kwargs:
        kwargs['roll_len'] = 40
    _rolling_player_f(player, 'G', **kwargs)


def rolling_player_cf(player, **kwargs):
    """
    Creates a graph with CF% and CF% off. Defaults to roll_len of 25.

    :param player: str or int, player to generate for
    :param kwargs: other filters. See scrapenhl2.plot.visualization_helper.get_and_filter_5v5_log for more information.

    :return: nothing, or figure
    """
    if 'roll_len' not in kwargs:
        kwargs['roll_len'] = 25
    _rolling_player_f(player, 'C', **kwargs)


def _rolling_player_f(player, gfcf, **kwargs):
    """
    Creates a graph with CF% or GF% (on plus off). Use gfcf to indicate which one.

    :param player: str or int, player to generate for
    :param gfcf: str. Use 'G' for GF% and GF% Off and 'C' for CF% and CF% Off
    :param kwargs: other filters. See scrapenhl2.plot.visualization_helper.get_and_filter_5v5_log for more information.

    :return: nothing, or figure
    """

    kwargs['player'] = player
    fa = vhelper.get_and_filter_5v5_log(**kwargs)

    df = pd.concat([fa[['Season', 'Game']], calculate_f_rates(fa, gfcf)], axis=1)
    col_dict = {col[col.index(' ') + 1:]: col for col in df.columns if '%' in col}

    plt.clf()

    df.loc[:, 'Game Number'] = 1
    df.loc[:, 'Game Number'] = df['Game Number'].cumsum()
    df.set_index('Game Number', inplace=True)

    label = gfcf + 'F%'
    plt.plot(df.index, df[col_dict[label]], label=label)
    label = gfcf + 'F% Off'
    plt.plot(df.index, df[col_dict[label]], label=label, ls='--')
    plt.legend(loc=1, fontsize=10)

    plt.title(_get_rolling_f_title(gfcf, **kwargs))

    # axes
    plt.xlabel('Game')
    plt.ylabel(gfcf + 'F%')
    plt.ylim(0.3, 0.7)
    plt.xlim(0, len(df))
    ticks = list(np.arange(0.3, 0.71, 0.05))
    plt.yticks(ticks, ['{0:.0f}%'.format(100 * tick) for tick in ticks])

    vhelper.savefilehelper(**kwargs)


def _calculate_f_rates(df, gfcf):
    """
    Calculates GF% or CF% (plus off)

    :param dataframe: dataframe
    :param gfcf: str. Use 'G' for GF% and GF% Off and 'C' for CF% and CF% Off

    :return: dataframe
    """

    # Select columns
    fa = df.filter(regex='\d{2}-game')
    cols_wanted = {gfcf + x for x in {'FON', 'FOFF', 'AON', 'AOFF'}}
    fa = fa.select(lambda colname: colname[colname.index(' ') + 1:] in cols_wanted, axis=1)

    # This is to help me select columns
    col_dict = {col[col.index(' ') + 1:]: col for col in fa.columns}

    # Transform
    prefix = col_dict[gfcf + 'FON'][:col_dict[gfcf + 'FON'].index(' ')]  # e.g. 25-game
    fa.loc[:, '{0:s} {1:s}F%'.format(prefix, gfcf)] = fa[col_dict[gfcf + 'FON']] / \
                                                      (fa[col_dict[gfcf + 'FON']] + fa[col_dict[gfcf + 'AON']])
    fa.loc[:, '{0:s} {1:s}F% Off'.format(prefix, gfcf)] = fa[col_dict[gfcf + 'FOFF']] / \
                                                          (fa[col_dict[gfcf + 'FOFF']] +
                                                           fa[col_dict[gfcf + 'AOFF']])

    # Keep only those columns
    fa = fa[['{0:s} {1:s}F%'.format(prefix, gfcf), '{0:s} {1:s}F% Off'.format(prefix, gfcf)]]

    return fa


def _get_rolling_f_title(gfcf, **kwargs):
    """
    Returns default title for this type of graph

    :param gfcf: str. Use 'G' for GF% and GF% Off and 'C' for CF% and CF% Off
    :param kwargs:

    :return: str, the title
    """

    title = 'Rolling {0:d}-game rolling {1:s}F% for {2:s}'.format(kwargs['roll_len'], gfcf,
                                                                  players.player_as_str(kwargs['player']))
    title += '\n{0:s} to {1:s}'.format(*(str(x) for x in vhelper.get_startdate_enddate_from_kwargs(**kwargs)))