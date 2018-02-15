"""
This module creates rolling CF% and GF% charts
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np  # standard scientific python stack
import pandas as pd  # standard scientific python stack

from scrapenhl2.scrape import players, schedules
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
        Use x='Date' to index on date instead of game number

    :return: nothing, or figure
    """

    kwargs['player'] = player
    fa = vhelper.get_and_filter_5v5_log(**kwargs)

    df = pd.concat([fa[['Season', 'Game']], _calculate_f_rates(fa, gfcf)], axis=1)
    col_dict = {col[col.index(' ') + 1:]: col for col in df.columns if '%' in col}

    plt.close('all')

    df.loc[:, 'Game Number'] = 1
    df.loc[:, 'Game Number'] = df['Game Number'].cumsum()
    df = df.set_index('Game Number', drop=False)

    if 'x' in kwargs and kwargs['x'] == 'Date':
        df = schedules.attach_game_dates_to_dateframe(df)
        df.loc[:, 'Date'] = pd.to_datetime(df.Date)
        #df.loc[:, 'Date'] = pd.to_datetime(df.Date).dt.strftime('%b/%y')
        df = df.set_index(pd.DatetimeIndex(df['Date']))
        plt.gca().xaxis_date()
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b\'%y'))
        plt.xlabel('Date')
    else:
        plt.xlabel('Game')
        kwargs['x'] = 'Game Number'

    series = gfcf + 'F%'
    series2 = gfcf + 'F% Off'

    # Avoid the long lines in offseason by setting first value in each season to None
    df.loc[:, 'PrevSeason'] = df.Season.shift(1)
    df.loc[:, 'PrevSeason'] = df.PrevSeason.fillna(df.Season - 1)
    df.loc[df.Season != df.PrevSeason, col_dict[series]] = None
    df.loc[df.Season != df.PrevSeason, col_dict[series2]] = None

    # Add YY-YY for top axis
    df.loc[:, 'TopLabel'] = df.Season.apply(lambda x: '{0:d}-{1:s} -->'.format(x, str(x+1)[2:]))

    plt.plot(df.index, df[col_dict[series]].values, label=series)
    plt.plot(df.index, df[col_dict[series2]].values, label=series2, ls='--')

    plt.legend(loc=1, fontsize=10)

    # Add seasons at top
    ax1 = plt.gca()
    ax2 = ax1.twiny()
    ax2.set_xlim(*ax1.get_xlim())
    temp = df[df.Season != df.PrevSeason][[kwargs['x'], 'TopLabel']]
    ax2.tick_params(length=0, labelsize=8)
    ax2.set_xticks(temp.iloc[:, 0].values)
    ax2.set_xticklabels(temp.iloc[:, 1].values)
    for label in ax2.xaxis.get_majorticklabels():
        label.set_horizontalalignment('left')
    for tick in ax2.xaxis.get_major_ticks():
        tick.set_pad(-10)

    plt.title(_get_rolling_f_title(gfcf, **kwargs))

    # axes

    plt.ylabel(gfcf + 'F%')
    plt.ylim(0.3, 0.7)
    plt.xlim(df.index.min(), df.index.max())
    ticks = list(np.arange(0.3, 0.71, 0.05))
    plt.yticks(ticks, ['{0:.0f}%'.format(100 * tick) for tick in ticks])

    return vhelper.savefilehelper(**kwargs)


def _calculate_f_rates(df, gfcf):
    """
    Calculates GF% or CF% (plus off)

    :param dataframe: dataframe
    :param gfcf: str. Use 'G' for GF% and GF% Off and 'C' for CF% and CF% Off

    :return: dataframe
    """

    # Select columns
    fa = df.filter(regex='\d+-game')
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
    return title