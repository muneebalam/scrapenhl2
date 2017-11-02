"""
This method contains utilities for visualization.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from scrapenhl2.scrape import schedules
from scrapenhl2.scrape import players
from scrapenhl2.manipulate import manipulate as manip
from scrapenhl2.scrape import general_helpers as helper


def format_number_with_plus(stringnum):
    """
    Converts 0 to 0, -1 to -1, and 1 to +1 (for presentation purposes).

    :param stringnum: int

    :return: str, transformed as specified above.
    """
    if stringnum <= 0:
        return str(stringnum)
    else:
        return '+' + str(stringnum)


def hex_to_rgb(value):
    """Return (red, green, blue) for the hex color given as #rrggbb."""
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))


def rgb_to_hex(red, green, blue):
    """Return color as #rrggbb for the given RGB color values."""
    return '#%02x%02x%02x' % (int(red), int(green), int(blue))


def make_color_darker(hex=None, rgb=None, returntype='hex'):
    """
    Makes specified color darker. This is done by converting to rgb and multiplying by 50%.

    :param hex: str. Specify either this or rgb.
    :param rgb: 3-tuple of floats 0-255. Specify either this or hex
    :param returntype: str, 'hex' or 'rgb'

    :return: a hex or rgb color, input color but darker
    """
    if hex is None and rgb is None:
        return
    if hex is not None:
        color = hex_to_rgb(hex)
    else:
        color = rgb

    color = [x * 0.5 for x in color]

    if returntype == 'rgb':
        return color
    return rgb_to_hex(*color)


def make_color_lighter(hex=None, rgb=None, returntype='hex'):
    """
    Makes specified color lighter. This is done by converting to rgb getting closer to 255 by 50%.

    :param hex: str. Specify either this or rgb.
    :param rgb: 3-tuple of floats 0-255. Specify either this or hex
    :param returntype: str, 'hex' or 'rgb'

    :return: a hex or rgb color, input color but lighter
    """
    if hex is None and rgb is None:
        return
    if hex is not None:
        color = hex_to_rgb(hex)
    else:
        color = rgb

    color = [255 - ((255 - x) * 0.5) for x in color]

    if returntype == 'rgb':
        return color
    return rgb_to_hex(*color)


def get_and_filter_5v5_log(**kwargs):
    """
    This method retrieves the 5v5 log and filters for keyword arguments provided to the original method.
    For example, rolling_player_cf calls this method first.

    Currently supported keyword arguments:

    - startseason: int, the season to start with. Defaults to current - 3.
    - startdate: str, yyyy-mm-dd. Defaults to Sep 15 of startseason
    - endseason: int, the season to end with (inclusive). Defaults to current
    - enddate: str, yyyy-mm-dd. Defaults to June 21 of endseason + 1
    - roll_len: int, calculates rolling sums over this variable.
    - player: int or str, player ID or name

    :param kwargs: e.g. startseason, endseason.

    :return: df, filtered
    """

    # TODO many of these methods can be moved to manip
    df = get_5v5_df_start_end(**kwargs)
    df = filter_5v5_for_player(df, **kwargs)
    df = make_5v5_rolling(df, **kwargs)

    return df


def make_5v5_rolling(df, **kwargs):
    """
    Takes rolling sums of numeric columns and concatenates onto the dataframe.
    Will exclude season, game, player, and team.

    :param df: dataframe
    :param kwargs: the relevant one is roll_len

    :return: dataframe with extra columns
    """
    if 'roll_len' in kwargs:
        roll_len = kwargs['roll_len']

        # Join key for later
        df.loc[:, 'Row'] = 1
        df.loc[:, 'Row'] = df.Row.cumsum()

        # Get df and roll
        to_exclude = {'Game', 'PlayerID', 'Season', 'Team'}
        numeric_df = df.select_dtypes(include=[np.number])
        numeric_df.drop(to_exclude, axis=1, inplace=True, errors='ignore')

        if 'ignore_missing' in kwargs and kwargs['ignore_missing'] is True:
            # Just do defaults
            rollingdf = numeric_df.rolling(roll_len).sum()
            rollingdf.loc[:, 'Row'] = 1
            rollingdf.loc[:, 'Row'] = rollingdf.Row.cumsum()
        else:
            # TODO in 5v5 player logs add missing games
            # Record played games
            numeric_df.loc[:, 'Row'] = 1
            numeric_df.loc[:, 'Row'] = numeric_df.Row.cumsum()
            games_played = numeric_df.dropna().Row
            numeric_df.drop('Row', axis=1, inplace=True)

            # Calculate rolling
            rollingdf = numeric_df.dropna().rolling(roll_len, min_periods=1).sum().assign(Row=games_played)

        # Rename columns
        columnnames = {col: '{0:d}-game {1:s}'.format(roll_len, col) for col in numeric_df.columns}
        rollingdf.rename(columns=columnnames, inplace=True)

        # Add back to original
        df2 = df.merge(rollingdf, how='left', on='Row').drop('Row', axis=1)
        return df2
    return df


def filter_5v5_for_player(df, **kwargs):
    """
    This method filters the given dataframe for given player(s), if specified

    :param df: dataframe

    :param kwargs: relevant one is player

    :return: dataframe, filtered for specified players
    """

    if 'player' in kwargs:
        playerid = players.player_as_id(kwargs['player'])
        df2 = df.query("PlayerID == {0:d}".format(playerid))
        return df2
    return df


def get_enddate_from_kwargs(**kwargs):
    """Returns 6/21 of endseason + 1, or enddate"""

    if 'enddate' in kwargs:
        return kwargs['enddate']
    elif 'endseason' in kwargs:
        return '{0:d}-06-21'.format(kwargs['endseason']+1)
    elif 'startseason' in kwargs:
        return get_enddate_from_kwargs(endseason=kwargs['startseason'])
    elif 'startdate' in kwargs:
        return get_enddate_from_kwargs(endseason=helper.infer_season_from_date(kwargs['startdate']))
    else:
        return get_enddate_from_kwargs(endseason=schedules.get_current_season())


def get_startdate_enddate_from_kwargs(**kwargs):
    """Returns startseason and endseason kwargs. Defaults to current - 3 and current"""

    enddate = get_enddate_from_kwargs(**kwargs)
    if 'startdate' in kwargs:
        startdate = kwargs['startdate']
    elif 'startseason' in kwargs:
        startdate = '{0:d}-09-15'.format(kwargs['startseason'])
    else:
        startdate = '{0:d}-09-15'.format(helper.infer_season_from_date(enddate) - 3)

    return startdate, enddate


def get_5v5_df_start_end(**kwargs):
    """
    This method retrieves the correct years of the 5v5 player log and concatenates them.

    :param kwargs: the relevant ones here are startseason and endseason

    :return: dataframe
    """

    startdate, enddate = get_startdate_enddate_from_kwargs(**kwargs)
    startseason, endseason = (helper.infer_season_from_date(x) for x in (startdate, enddate))

    df = []
    for season in range(startseason, endseason + 1):
        temp = manip.get_5v5_player_log(season)
        sch = schedules.get_season_schedule(season)

        temp = temp.merge(sch[['Game', 'Date']], how='left', on='Game')
        temp = temp[(temp.Date >= startdate) & (temp.Date <= enddate)]
        temp = temp.assign(Season=season)
        df.append(temp)
    df = pd.concat(df).sort_values(['Date']).drop('Date', axis=1)  # When games rescheduled, Game ID not in order.
    return df


def savefilehelper(**kwargs):
    """
    Saves current matplotlib figure, or saves to file, or displays

    :param kwargs: searches for 'save_file'. If not found or None, displays figure. If 'fig', returns figure.
    If a filepath, saves.

    :return: nothing, or a figure
    """

    save_file = None if 'save_file' not in kwargs else kwargs['save_file']
    if save_file is None:
        plt.show()
    elif save_file == 'fig':
        return plt.gcf()
    else:
        plt.savefig(save_file)
    plt.close()


if __name__ == '__main__':
    from scrapenhl2.plot import game_timeline as gt
    from scrapenhl2.plot import game_h2h as gh
    gt.live_timeline('PHI', 'ARI', False)
    # gh.live_h2h('PHI', 'ARI', False)
