"""
This module contains methods for creating the rolling boxcars stacked area graph.
"""

import pandas as pd
import matplotlib.pyplot as plt

from scrapenhl2.plot import visualization_helper as vhelper
from scrapenhl2.scrape import players

def rolling_player_boxcars(player, **kwargs):
    """
    A method to generate the rolling boxcars graph.

    :param player: str or int, player to generate for
    :param kwargs: other filters. See scrapenhl2.plot.visualization_helper.get_and_filter_5v5_log for more information.

    :return: nothing, or figure
    """

    kwargs['player'] = player
    if 'roll_len' not in kwargs:
        kwargs['roll_len'] = 25
    boxcars = vhelper.get_and_filter_5v5_log(**kwargs)

    boxcars = pd.concat([boxcars[['Season', 'Game']], calculate_boxcar_rates(boxcars)], axis=1)

    col_dict = {col[col.index(' ') + 1:col.index('/') ]: col for col in boxcars.columns if col[-3:] == '/60'}

    plt.clf()

    # Set an index
    # TODO allow for datetime index
    boxcars.loc[:, 'Game Number'] = 1
    boxcars.loc[:, 'Game Number'] = boxcars['Game Number'].cumsum()
    boxcars.set_index('Game Number', inplace=True)
    plt.fill_between(boxcars.index, 0, boxcars[col_dict['iG']], label='G', color='k')
    plt.fill_between(boxcars.index, boxcars[col_dict['iG']], boxcars[col_dict['iP1']], label='A1', color='b')
    plt.fill_between(boxcars.index, boxcars[col_dict['iG']], boxcars[col_dict['iP']], label='A2', color='dodgerblue')
    plt.fill_between(boxcars.index, boxcars[col_dict['iP']], boxcars[col_dict['GFON']],
                     label='Other\nGFON', color='c', alpha=0.3)

    plt.xlabel('Game')
    plt.ylabel('Per 60')
    plt.xlim(0, len(boxcars))
    plt.ylim(0, 4)

    position = players.get_player_position(player)
    if position == 'D':
        ypos = [0.17, 0.84, 2.5]
        ytext = ['P1\nG', 'P1\nP', 'P1\nGF']
    elif position in {'C', 'R', 'L', 'F'}:
        ypos = [0.85, 1.94, 2.7]
        ytext = ['L1\nG', 'L1\nP', 'L1\nGF']

    xlimits = plt.xlim()
    tempaxis = plt.twinx()
    tempaxis.tick_params(axis='y', which='major', pad=2)
    tempaxis.set_yticks(ypos)
    tempaxis.set_yticklabels(ytext, fontsize=8)
    tempaxis.grid(b=False)
    tempaxis.plot(xlimits, [ypos[0], ypos[0]], color='k', ls=':')
    tempaxis.plot(xlimits, [ypos[1], ypos[1]], color='dodgerblue', ls=':')
    tempaxis.plot(xlimits, [ypos[2], ypos[2]], color='c', ls=':')

    plt.legend(loc=2, bbox_to_anchor=(1.05, 1), fontsize=10)
    tempaxis.set_ylim(0, 4)
    plt.xlim(0, len(boxcars))
    plt.ylim(0, 4)

    plt.title(_get_rolling_boxcars_title(**kwargs))

    return vhelper.savefilehelper(**kwargs)


def _get_rolling_boxcars_title(**kwargs):
    """
    Returns suggested chart title for rolling boxcar graph given these keyword arguments

    :param kwargs:

    :return: str
    """

    title = 'Rolling {0:d}-game boxcar rates for {1:s}'.format(kwargs['roll_len'],
                                                               players.player_as_str(kwargs['player']))
    title += '\n{0:s} to {1:s}'.format(*(str(x) for x in vhelper.get_startdate_enddate_from_kwargs(**kwargs)))
    return title


def calculate_boxcar_rates(df):
    """
    Takes the given dataframe and makes the following calculations:

    - Divides col ending in GFON, iA2, iA1, and iG by one ending in TOI
    - Adds iG to iA1, calls result iP1
    - Adds iG and iA1 to iA2, calls result iP
    - Adds /60 to ends of iG, iA1, iP1, iA2, iP, and GFON

    :param df: dataframe

    :return: dataframe with columns changed as specified, and only those mentioned above selected.
    """

    # Select columns
    boxcars = df.filter(regex='\d{2}-game')
    cols_wanted = {'GFON', 'iA1', 'iA2', 'iG', 'TOION'}
    boxcars = boxcars.select(lambda colname: colname[colname.index(' ') + 1:] in cols_wanted, axis=1)

    # This is to help me select columns
    col_dict = {col[col.index(' ') + 1:]: col for col in boxcars.columns}

    # Transform
    for col in {'iG', 'iA1', 'iA2', 'GFON'}:
        boxcars.loc[:, col_dict[col]] = boxcars[col_dict[col]] / boxcars[col_dict['TOION']]

    prefix = col_dict['iG'][:col_dict['iG'].index(' ')]  # e.g. 25-game
    boxcars.loc[:, prefix + ' iP1'] = boxcars[col_dict['iG']] + boxcars[col_dict['iA1']]
    boxcars.loc[:, prefix + ' iP'] = boxcars[prefix + ' iP1'] + boxcars[col_dict['iA2']]

    # Rename
    renaming = {col: col + '/60' for col in boxcars.columns if col[-5:] != 'TOION'}
    boxcars.rename(columns=renaming, inplace=True)

    return boxcars


if __name__ == '__main__':
    rolling_player_boxcars(player='Nicklas Backstrom')
    # First few games are blank: why?

