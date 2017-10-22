import matplotlib.pyplot as plt
import numpy as np  # standard scientific python stack
import pandas as pd  # standard scientific python stack

import scrapenhl2.manipulate.manipulate as manip
import scrapenhl2.scrape.scrape_setup as ss  # lots of helpful methods in this module


def rolling_player_cf(player, roll_len=25, startseason=None, endseason=None, save_file=None):
    """
    Creates a graph with CF% and CF% off.
    :param players: int or str
    :param startseason: int, the season to start (inclusive). Defaults to 3 years ago.
    :param endseason: int, the season to finish (inclusive). If not specified, startseason. Else, current.
    :param save_file: str to save, or None to show. Or 'fig' to return figure
    :return: nothing, or figure
    """
    plt.clf()
    if startseason is None and endseason is None:
        endseason = ss.get_current_season()
        startseason = endseason - 3
    elif startseason is None:
        startseason = endseason
    elif endseason is None:
        endseason = startseason

    playerid = ss.player_as_id(player)
    df = []
    for season in range(startseason, endseason + 1):
        temp = manip.get_player_5v5_log(season)
        temp = temp.query("PlayerID == {0:d}".format(playerid))
        temp = temp.assign(Season=season)
        df.append(temp)
    df = pd.concat(df).sort_values(['Season', 'Game'])

    columnnames = {col: '{0:d}-game {1:s}'.format(roll_len, col) for col in \
                   ['CFON', 'CAON', 'CFOFF', 'CAOFF']}
    columnnames2 = {col: '{0:d}-game {1:s}'.format(roll_len, col) for col in ['CF%', 'CF Off%']}
    df.loc[:, 'CFOFF'] = df.TeamCF - df.CFON
    df.loc[:, 'CAOFF'] = df.TeamCA - df.CAON

    # Calculate rolling numbers
    rollingdf = df[list(columnnames.keys())].rolling(roll_len).sum()

    # The first roll_len entries will be NaN--need to fillna using cumsum
    for col in rollingdf:
        rollingdf.loc[:, col] = rollingdf[col].fillna(df[col].cumsum())

    # Rename to new column names
    rollingdf.rename(columns=columnnames, inplace=True)

    # Add to old df
    df = pd.concat([df, rollingdf], axis=1)

    df.loc[:, '{0:d}-game CF%'.format(roll_len)] = \
        df['{0:d}-game CFON'.format(roll_len)] / (df['{0:d}-game CFON'.format(roll_len)] + \
                                                  df['{0:d}-game CAON'.format(roll_len)])
    df.loc[:, '{0:d}-game CF Off%'.format(roll_len)] = \
        df['{0:d}-game CFOFF'.format(roll_len)] / (df['{0:d}-game CFOFF'.format(roll_len)] + \
                                                  df['{0:d}-game CAOFF'.format(roll_len)])

    df.loc[:, 'Game Number'] = 1
    df.loc[:, 'Game Number'] = df['Game Number'].cumsum()

    label = 'CF%'
    plt.plot(df['Game Number'], df[columnnames2[label]], label=label)
    label = 'CF Off%'
    plt.plot(df['Game Number'], df[columnnames2[label]], label=label, ls='--')
    plt.legend(loc=1, fontsize=10)

    plt.title(_get_rolling_cf_title(player, roll_len, startseason, endseason))

    # axes
    plt.xlabel('Game')
    plt.ylabel('CF%')
    plt.ylim(0.3, 0.7)
    plt.xlim(0, len(df))
    ticks = list(np.arange(0.3, 0.71, 0.05))
    plt.yticks(ticks, ['{0:.0f}%'.format(100 * tick) for tick in ticks])

    if save_file is None:
        plt.show()
    elif save_file == 'fig':
        return plt.gcf()
    else:
        plt.savefig(save_file)


def _get_rolling_cf_title(player, roll_len, startseason, endseason):
    """
    Returns default title for this type of graph
    :param player: int or str, the player
    :param roll_len: int, number of games in rolling window
    :param startseason: int, starting season
    :param endseason: int, ending season
    :return:
    """

    player = ss.player_as_str(ss.player_as_id(player))
    return '{0:d}-game rolling CF% for {1:s}, {2:d}-{3:d}'.format(roll_len, player, startseason, endseason + 1)


