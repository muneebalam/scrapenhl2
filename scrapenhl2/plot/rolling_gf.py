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

    kwargs['player'] = player
    if 'roll_len' not in kwargs:
        kwargs['roll_len'] = 40
    gfga = vhelper.get_and_filter_5v5_log(**kwargs)

    df = pd.concat([gfga[['Season', 'Game']], calculate_gf_rates(gfga)], axis=1)
    col_dict = {col[col.index(' ') + 1:]: col for col in df.columns if '%' in col}

    plt.clf()

    df.loc[:, 'Game Number'] = 1
    df.loc[:, 'Game Number'] = df['Game Number'].cumsum()
    df.set_index('Game Number', inplace=True)

    label = 'GF%'
    plt.plot(df.index, df[col_dict[label]], label=label)
    label = 'GF% Off'
    plt.plot(df.index, df[col_dict[label]], label=label, ls='--')
    plt.legend(loc=1, fontsize=10)

    plt.title(_get_rolling_gf_title(**kwargs))

    # axes
    plt.xlabel('Game')
    plt.ylabel('GF%')
    plt.ylim(0.3, 0.7)
    plt.xlim(0, len(df))
    ticks = list(np.arange(0.3, 0.71, 0.05))
    plt.yticks(ticks, ['{0:.0f}%'.format(100 * tick) for tick in ticks])

    vhelper.savefilehelper(**kwargs)


def calculate_gf_rates(df):
    """
    Calculates GF% and GF% Off

    :param dataframe: dataframe

    :return: dataframe
    """

    # Select columns
    gfga = df.filter(regex='\d{2}-game')
    cols_wanted = {'GFON', 'GFOFF', 'GAON', 'GAOFF'}
    gfga = gfga.select(lambda colname: colname[colname.index(' ') + 1:] in cols_wanted, axis=1)

    # This is to help me select columns
    col_dict = {col[col.index(' ') + 1:]: col for col in gfga.columns}

    # Transform
    prefix = col_dict['GFON'][:col_dict['GFON'].index(' ')]  # e.g. 25-game
    gfga.loc[:, prefix + ' GF%'] = gfga[col_dict['GFON']] / (gfga[col_dict['GFON']] + gfga[col_dict['GAON']])
    gfga.loc[:, prefix + ' GF% Off'] = gfga[col_dict['GFOFF']] / (gfga[col_dict['GFOFF']] + gfga[col_dict['GAOFF']])

    # Keep only those columns
    gfga = gfga[[prefix + ' GF%', prefix + ' GF% Off']]

    return gfga


def _get_rolling_gf_title(**kwargs):
    """
    Returns default title for this type of graph

    :param kwargs:

    :return: str, the title
    """

    title = 'Rolling {0:d}-game rolling GF% for {1:s}'.format(kwargs['roll_len'],
                                                              players.player_as_str(kwargs['player']))
    title += '\n{0:s} to {1:s}'.format(*(str(x) for x in vhelper.get_startdate_enddate_from_kwargs(**kwargs)))
    return title


if __name__ == '__main__':
    rolling_player_gf(player='Nicklas Backstrom')



