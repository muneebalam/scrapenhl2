"""
This module contains methods to generate a graph showing player CF%. 18 little graphs, 1 for each of 18 players.
"""

import matplotlib.pyplot as plt
import pandas as pd

import scrapenhl2.scrape.players as players
import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.general_helpers as helper
import scrapenhl2.manipulate.manipulate as manip
import scrapenhl2.plot.visualization_helper as vhelper
import scrapenhl2.plot.rolling_cf_gf as rolling_cfgf


def team_lineup_cf_graph(team, **kwargs):
    """
    This method builds a 4x5 matrix of rolling CF% line graphs. The left 4x3 matrix are forward lines and the top-right
    3x2 are defense pairs.

    :param team: str or id, team to build this graph for
    :param kwargs: need to specify the following as iterables of names: l1, l2, l3, l4, p1, p2, p3.
        Three players for each of the 'l's and two for each of the 'p's.

    :return: figure, or nothing
    """
    allplayers = []
    if 'l1' in kwargs and 'l2' in kwargs and 'l3' in kwargs and 'l4' in kwargs and \
                    'p1' in kwargs and 'p2' in kwargs and 'p3' in kwargs:
        # Change all to IDs
        # Go on this strange order because it'll be the order of the plots below
        for key in ['l1', 'p1', 'l2', 'p2', 'l3', 'p3', 'l4']:
            kwargs[key] = [players.player_as_id(x) for x in kwargs[key]]
            allplayers += kwargs[key]
    else:
        # TODO Find most common lines
        # Edit get_line_combos etc from manip, and the method to get player order from game_h2h, to work at team level
        pass

    # Get data
    kwargs['add_missing_games'] = True
    kwargs['team'] = team
    kwargs['players'] = allplayers
    if 'roll_len' not in kwargs:
        kwargs['roll_len'] = 25
    data = vhelper.get_and_filter_5v5_log(**kwargs)
    df = pd.concat([data[['Season', 'Game', 'PlayerID']], rolling_cfgf._calculate_f_rates(data, 'C')], axis=1)
    col_dict = {col[col.index(' ') + 1:]: col for col in df.columns if '%' in col}

    # Set up figure to share x and y
    fig, axes = plt.subplots(4, 5, sharex=True, sharey=True, figsize=[12, 8])

    # Make chart for each player
    gamenums = df[['Season', 'Game']].drop_duplicates().assign(GameNum=1)
    gamenums.loc[:, 'GameNum'] = gamenums.GameNum.cumsum()
    df = df.merge(gamenums, how='left', on=['Season', 'Game'])

    axes = axes.flatten()
    for i in range(len(allplayers)):
        ax = axes[i]
        ax.set_title(players.player_as_str(allplayers[i]), fontsize=10)
        temp = df.query('PlayerID == {0:d}'.format(int(allplayers[i])))
        x = temp.GameNum.values
        y1 = temp[col_dict['CF%']].values
        y2 = temp[col_dict['CF% Off']].values
        ax.fill_between(x, y1, y2, where=y1 > y2, alpha=0.5)
        ax.fill_between(x, y1, y2, where=y2 > y1, alpha=0.5)
        ax.plot(x, y1)
        ax.plot(x, y2, ls='--')
        ax.plot(x, [0.5 for _ in range(len(x))], color='k')

    for i, ax in enumerate(axes):
        for direction in ['right', 'top', 'bottom', 'left']:
            ax.spines[direction].set_visible(False)
        ax.xaxis.set_ticks_position('none')
        ax.yaxis.set_ticks_position('none')

    # Set title and axis labels
    axes[0].set_ylim(0.35, 0.65)
    axes[0].set_yticks([0.4, 0.5, 0.6])
    axes[0].set_yticklabels(['40%', '50%', '60%'])
    axes[0].set_xlim(1, df.GameNum.max())

    plt.annotate('Game', xy=(0.5, 0.05), ha='center', va='top', xycoords='figure fraction')

    fig.suptitle(_team_lineup_cf_graph_title(**kwargs), fontsize=16, y=0.95)

    # Return
    return vhelper.savefilehelper(**kwargs)


def _team_lineup_cf_graph_title(**kwargs):
    return ', '.join(vhelper.generic_5v5_log_graph_title('Lineup CF%', **kwargs))
