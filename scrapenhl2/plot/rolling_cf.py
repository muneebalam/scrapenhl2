import matplotlib.pyplot as plt
import numpy as np  # standard scientific python stack
import pandas as pd  # standard scientific python stack

import scrapenhl2.manipulate.manipulate as manip
from scrapenhl2.scrape import players
from scrapenhl2.scrape import schedules
from scrapenhl2.plot import visualization_helper as vhelper


def rolling_player_cf(player, **kwargs):
    """
    Creates a graph with CF% and CF% off.

    :param player: str or int, player to generate for
    :param kwargs: other filters. See scrapenhl2.plot.visualization_helper.get_and_filter_5v5_log for more information.

    :return: nothing, or figure
    """

    kwargs['player'] = player
    if 'roll_len' not in kwargs:
        kwargs['roll_len'] = 25
    cfca = vhelper.get_and_filter_5v5_log(**kwargs)

    df = pd.concat([cfca[['Season', 'Game']], calculate_cf_rates(cfca)], axis=1)
    col_dict = {col[col.index(' ') + 1:]: col for col in df.columns if '%' in col}

    plt.clf()

    df.loc[:, 'Game Number'] = 1
    df.loc[:, 'Game Number'] = df['Game Number'].cumsum()
    df.set_index('Game Number', inplace=True)

    label = 'CF%'
    plt.plot(df.index, df[col_dict[label]], label=label)
    label = 'CF% Off'
    plt.plot(df.index, df[col_dict[label]], label=label, ls='--')
    plt.legend(loc=1, fontsize=10)

    plt.title(_get_rolling_cf_title(**kwargs))

    # axes
    plt.xlabel('Game')
    plt.ylabel('CF%')
    plt.ylim(0.3, 0.7)
    plt.xlim(0, len(df))
    ticks = list(np.arange(0.3, 0.71, 0.05))
    plt.yticks(ticks, ['{0:.0f}%'.format(100 * tick) for tick in ticks])

    vhelper.savefilehelper(**kwargs)


def calculate_cf_rates(df):
    """
    Calculates CF% and CF% Off

    :param dataframe: dataframe

    :return: dataframe
    """

    # Select columns
    cfca = df.filter(regex='\d{2}-game')
    cols_wanted = {'CFON', 'CFOFF', 'CAON', 'CAOFF'}
    cfca = cfca.select(lambda colname: colname[colname.index(' ') + 1:] in cols_wanted, axis=1)

    # This is to help me select columns
    col_dict = {col[col.index(' ') + 1:]: col for col in cfca.columns}

    # Transform
    prefix = col_dict['CFON'][:col_dict['CFON'].index(' ')]  # e.g. 25-game
    cfca.loc[:, prefix + ' CF%'] = cfca[col_dict['CFON']] / (cfca[col_dict['CFON']] + cfca[col_dict['CAON']])
    cfca.loc[:, prefix + ' CF% Off'] = cfca[col_dict['CFOFF']] / (cfca[col_dict['CFOFF']] + cfca[col_dict['CAOFF']])

    # Keep only those columns
    cfca = cfca[[prefix + ' CF%', prefix + ' CF% Off']]

    return cfca


def _get_rolling_cf_title(**kwargs):
    """
    Returns default title for this type of graph

    :param kwargs:

    :return: str, the title
    """

    title = 'Rolling {0:d}-game rolling CF% for {1:s}'.format(kwargs['roll_len'],
                                                              players.player_as_str(kwargs['player']))
    title += '\n{0:s} to {1:s}'.format(*(str(x) for x in vhelper.get_startdate_enddate_from_kwargs(**kwargs)))
    return title


if __name__ == '__main__':
    rolling_player_cf(player='Nicklas Backstrom')
    # First few games are blank: why?



