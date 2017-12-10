"""
This method contains utilities for visualization.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

from scrapenhl2.scrape import schedules, players, team_info
from scrapenhl2.manipulate import manipulate as manip
from scrapenhl2.scrape import general_helpers as helper
import scrapenhl2.plot.label_lines as label_lines


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


def hex_to_rgb(value, maxval=256):
    """Return (red, green, blue) for the hex color given as #rrggbb."""
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16)/256*maxval for i in range(0, lv, lv // 3))


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
    - roll_len_days: int, calculates rolling sum over this time window
    - player: int or str, player ID or name
    - players: list of int or str, player IDs or names
    - min_toi: float, minimum TOI for a player for inclusion in minutes.
    - max_toi: float, maximum TOI for a player for inclusion in minutes.
    - min_toi60: float, minimum TOI60 for a player for inclusion in minutes.
    - max_toi60: float, maximum TOI60 for a player for inclusion in minutes.
    - team: int or str, filter data for this team only
    - add_missing_games: bool. If True will add in missing rows for missing games. Must also specify team.

    Developer's note: when adding support for new kwargs, also add support in _generic_graph_title

    :param kwargs: e.g. startseason, endseason.

    :return: df, filtered
    """

    # TODO many of these methods can be moved to manip
    df = get_5v5_df_start_end(**kwargs)
    df = filter_5v5_for_team(df, **kwargs)
    df = filter_5v5_for_player(df, **kwargs)
    df = make_5v5_rolling_gp(df, **kwargs)
    df = make_5v5_rolling_days(df, **kwargs)
    df = insert_missing_team_games(df, **kwargs)
    df = filter_5v5_for_toi(df, **kwargs)

    return df


def insert_missing_team_games(df, **kwargs):
    """

    :param df: dataframe, 5v5 player log or part of it
    :param kwargs: relevant ones are 'team' and 'add_missing_games'

    :return: dataframe with added rows
    """
    if 'add_missing_games' in kwargs and 'team' in kwargs and kwargs['add_missing_games'] is True:
        startdate, enddate = get_startdate_enddate_from_kwargs(**kwargs)
        df2 = manip.convert_to_all_combos(df, np.NaN, ('Season', 'Game'), 'PlayerID')
        df2 = schedules.attach_game_dates_to_dateframe(df2).sort_values('Date')
        # Don't use the team kwarg here but this will obviously be messy if we bring in multiple teams' games
        # And get_and_filter_5v5_log does filter for team up above
        return df2
    return df


def make_5v5_rolling_days(df, **kwargs):
    """
    Takes rolling sums based on roll_len_days kwarg. E.g. 30 for a ~monthly rolling sum.

    :param df: dataframe
    :param kwargs: the relevant one is roll_len_days, int

    :return: dataframe with extra columns
    """
    if 'roll_len_days' in kwargs:
        roll_len = kwargs['roll_len_days']

        # Join to schedules to get game dates
        df2 = schedules.attach_game_dates_to_dateframe(df)

        # Join to a dataframe full of days
        # TODO use grouper to speed this up
        daysdf = pd.DataFrame({'Date': [df2.Date.min(), df2.Date.max()]}) \
            .assign(JoinKey=1) \
            .set_index('Date') \
            .asfreq('1D').reset_index() \
            .assign(JoinKey=1)
        playersdf = df2[['PlayerID']].drop_duplicates() \
            .assign(JoinKey=1) \
            .merge(daysdf, how='inner', on='JoinKey') \
            .drop('JoinKey', axis=1)
        playersdf.loc[:, 'Date'] = playersdf.Date.dt.strftime('%Y-%m-%d')
        fulldf = playersdf.merge(df2, how='left', on=['PlayerID', 'Date'])

        to_exclude = {'Game', 'Season', 'Team'}  # Don't want to sum these, even though they're numeric
        numeric_df = df.select_dtypes(include=[np.number])
        numeric_df = numeric_df.drop(to_exclude, axis=1, errors='ignore')

        rolling_df = fulldf[numeric_df.columns] \
            .groupby('PlayerID').rolling(roll_len, min_periods=1).sum() \
            .drop('PlayerID', axis=1) \
            .reset_index()

        assert len(rolling_df) == len(fulldf)

        # Rename columns
        columnnames = {col: '{0:d}-day {1:s}'.format(roll_len, col) for col in numeric_df.columns}
        rolling_df = rolling_df.rename(columns=columnnames)

        finaldf = pd.concat([fulldf, rolling_df], axis=1).dropna(subset={'Game'}).drop('Date', axis=1)
        return finaldf

    return df


def make_5v5_rolling_gp(df, **kwargs):
    """
    Takes rolling sums of numeric columns and concatenates onto the dataframe.
    Will exclude season, game, player, and team.

    :param df: dataframe
    :param kwargs: the relevant one is roll_len

    :return: dataframe with extra columns
    """
    if 'roll_len' in kwargs:
        roll_len = kwargs['roll_len']

        df = schedules.attach_game_dates_to_dateframe(df) \
            .sort_values(['PlayerID', 'Date']) \
            .drop('Date', axis=1)  # Need this to be in order, else the groupby-cumsum below won't work right

        # Get df and roll
        to_exclude = {'Game', 'Season', 'Team'}
        numeric_df = df.select_dtypes(include=[np.number])
        # Sometimes PlayerID gets converted to obj at some point, so just make sure it gets included
        # if 'PlayerID' not in numeric_df.columns:
        #     numeric_df.loc[:, 'PlayerID'] = df.PlayerID
        numeric_df = numeric_df.drop(to_exclude, axis=1, errors='ignore')
        rollingdf = numeric_df.groupby('PlayerID') \
            .rolling(roll_len, min_periods=1).sum() \
            .drop('PlayerID', axis=1) \
            .reset_index() \
            .drop('level_1', axis=1)

        # Rename columns
        columnnames = {col: '{0:d}-game {1:s}'.format(roll_len, col) for col in numeric_df.columns
                       if not col == 'PlayerID'}
        rollingdf = rollingdf.rename(columns=columnnames)

        # Add back to original
        # Order of players can change, so we'll assign row numbers in each player group
        df.loc[:, '_Row'] = 1
        df.loc[:, '_Row'] = df[['PlayerID', '_Row']].groupby('PlayerID').cumsum()
        rollingdf.loc[:, '_Row'] = 1
        rollingdf.loc[:, '_Row'] = rollingdf[['PlayerID', '_Row']].groupby('PlayerID').cumsum()
        df2 = df.merge(rollingdf, how='left', on=['PlayerID', '_Row']).drop('_Row', axis=1)
        return df2
    return df


def filter_5v5_for_toi(df, **kwargs):
    """
    This method filters the given dataframe for minimum or max TOI or TOI60.

    This method groups at the player level. So if a player hits the minimum total but not for one or more teams
    they played for over the the relevant time period, they will be included.

    :param df: dataframe

    :param kwargs: relevant ones are min_toi, max_toi, min_toi60, and max_toi60

    :return: dataframe, filtered for specified players
    """
    toitotals = df[['PlayerID', 'TOION', 'TOIOFF']].groupby('PlayerID', as_index=False).sum()
    toitotals.loc[:, 'TOI60'] = toitotals.TOION / (toitotals.TOION + toitotals.TOIOFF)

    if 'min_toi' in kwargs:
        toitotals = toitotals.query('TOION >= {0:f}'.format(kwargs['min_toi'] / 60))  # TOION is in hrs; min_toi in mins
    if 'max_toi' in kwargs:
        toitotals = toitotals.query('TOION <= {0:f}'.format(kwargs['max_toi'] / 60))
    if 'min_toi60' in kwargs:
        toitotals = toitotals.query('TOI60 >= {0:f}'.format(kwargs['min_toi60']))
    if 'max_toi60' in kwargs:
        toitotals = toitotals.query('TOI60 <= {0:f}'.format(kwargs['max_toi60']))

    df2 = df.merge(toitotals[['PlayerID']], how='inner', on='PlayerID')
    return df2


def filter_5v5_for_team(df, **kwargs):
    """
    This method filters the given dataframe for given team(s), if specified

    :param df: dataframe

    :param kwargs: relevant one is team

    :return: dataframe, filtered for specified players
    """

    if 'team' in kwargs:
        teamid = team_info.team_as_id(kwargs['team'])
        df2 = df.query("TeamID == {0:d}".format(teamid))
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
    if 'players' in kwargs:
        pids = players.playerlst_as_id(list(set(kwargs['players'])))
        # When merging float and int cols resulting column is object, so cast to float first
        df2 = df.merge(pd.DataFrame({'PlayerID': pids}).astype(float), how='inner', on='PlayerID')
        return df2
    return df


def get_enddate_from_kwargs(**kwargs):
    """Returns 6/21 of endseason + 1, or enddate"""

    if 'enddate' in kwargs:
        return kwargs['enddate']
    elif 'endseason' in kwargs:
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        return min('{0:d}-06-21'.format(kwargs['endseason']+1), today)
    elif 'startseason' in kwargs:
        return get_enddate_from_kwargs(endseason=kwargs['startseason'])
    elif 'season' in kwargs:
        return get_enddate_from_kwargs(endseason=kwargs['season'])
    elif 'startdate' in kwargs:
        return get_enddate_from_kwargs(endseason=helper.infer_season_from_date(kwargs['startdate']))
    else:
        return get_enddate_from_kwargs(endseason=schedules.get_current_season())


def get_startdate_enddate_from_kwargs(**kwargs):
    """Returns startseason and endseason kwargs. Defaults to current - 3 and current"""

    enddate = get_enddate_from_kwargs(**kwargs)
    if 'last_n_days' in kwargs:
        enddate2 = datetime.datetime(*[int(x) for x in enddate.split('-')])
        startdate2 = enddate2 - datetime.timedelta(days=kwargs['last_n_days'])
        startdate = startdate2.strftime('%Y-%m-%d')
    elif 'startdate' in kwargs:
        startdate = kwargs['startdate']
    elif 'startseason' in kwargs:
        startdate = '{0:d}-09-15'.format(kwargs['startseason'])
    elif 'season' in kwargs:
        startdate = '{0:d}-09-15'.format(kwargs['season'])
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


def generic_5v5_log_graph_title(figtype, **kwargs):
    """
    Generates a figure title incorporating parameters from kwargs:

    [Fig type] for [player, or multiple players, or team]
    [date range]
    [rolling window, if applicable]
    [TOI range, if applicable]
    [TOI60 range, if applicable]

    Methods for individual graphs can take this list and arrange as necessary.

    :param figtype: str brief description, e.g. Rolling CF% or Lineup CF%
    :param kwargs: See get_and_filter_5v5_log

    :return: list of strings
    """

    titlestr = []
    line1help = ''
    if 'player' in kwargs:
        line1help = ' for {0:s}'.format(players.player_as_str(kwargs['player']))
    elif 'team' in kwargs:
        line1help = ' for {0:s}'.format(team_info.team_as_str(kwargs['team']))
    elif 'players' in kwargs:
        line1help = ' for multiple players'
    titlestr.append('{0:s}{1:s}'.format(figtype, line1help))
    titlestr.append('{0:s} to {1:s}'.format(*get_startdate_enddate_from_kwargs(**kwargs)))
    if 'roll_len' in kwargs:
        titlestr.append('{0:d}-game moving window'.format(kwargs['roll_len']))
    elif 'roll_len' in kwargs:
        titlestr.append('{0:d}-day moving window'.format(kwargs['roll_len_days']))

    if 'min_toi' in kwargs and 'max_toi' in kwargs:
        titlestr.append('TOI range: {0:.1f}-{1:.1f} min'.format(kwargs['min_toi'], kwargs['max_toi']))
    elif 'min_toi' in kwargs:
        titlestr.append('TOI range: {0:.1f}+ min'.format(kwargs['min_toi']))
    elif 'min_toi' in kwargs:
        titlestr.append('TOI range: <= {0:.1f} min'.format(kwargs['max_toi']))

    if 'min_toi60' in kwargs and 'max_toi60' in kwargs:
        titlestr.append('TOI60 range: {0:.1f}-{1:.1f} min'.format(kwargs['min_toi60'], kwargs['max_toi60']))
    elif 'min_toi60' in kwargs:
        titlestr.append('TOI60 range: {0:.1f}+ min'.format(kwargs['min_toi60']))
    elif 'min_toi60' in kwargs:
        titlestr.append('TOI60 range: <= {0:.1f} min'.format(kwargs['max_toi60']))

    return titlestr


def parallel_coords(backgrounddf, foregrounddf, groupcol, legendcol=None, axis=None):
    """

    :param backgrounddf:
    :param foregrounddf:
    :param groupcol: For inline labels (e.g. initials)
    :param legendcol: So you can provide another groupcol for legend (e.g. name)
    :param axis:

    :return:
    """

    if axis is None:
        axis = plt.gca()
    if legendcol is not None:
        parallel_coords_background(backgrounddf.drop(legendcol, axis=1), groupcol, axis)
        parallel_coords_foreground(foregrounddf.drop(legendcol, axis=1), groupcol, axis)
        axis.legend(loc='upper left', col=2, fontsize=10)
    if legendcol is None:
        parallel_coords_background(backgrounddf, groupcol, axis)
        parallel_coords_foreground(foregrounddf, groupcol, axis)

    label_lines.labelLines(axis.get_lines(), zorder=3, fontsize=16)

    #for line, newlabel in zip(axis.get_lines(), foregrounddf[legendcol]):
    #    line.set_label(newlabel)


def parallel_coords_background(dataframe, groupcol, axis=None):
    """

    :param dataframe:
    :param groupcol:
    :param axis:
    :param zorder:
    :param alpha:
    :param color:
    :param label:
    :return:
    """

    if axis is None:
        axis = plt.gca()

    cols, df = parallel_coords_xy(dataframe, groupcol)
    for groupval in df[groupcol].value_counts().index:
        group = df.query('{0:s} == "{1:s}"'.format(groupcol, str(groupval)))
        axis.plot(group.X, group.Y, zorder=3, color='lightgray', alpha=0.5, label='_nolegend')

    xtickvals = list(cols.keys())
    xtickvals = list(range(min(xtickvals), max(xtickvals) + 1))
    axis.set_xticks(xtickvals)
    axis.set_xticklabels([cols[x] for x in xtickvals])


def parallel_coords_foreground(dataframe, groupcol, axis=None):
    """

    :param dataframe:
    :param groupcol:
    :param axis:
    :param zorder:
    :param alpha:
    :param color:
    :param label:
    :return:
    """

    if axis is None:
        axis = plt.gca()

    cols, df = parallel_coords_xy(dataframe, groupcol)
    for groupval in df[groupcol].value_counts().index:
        group = df.query('{0:s} == "{1:s}"'.format(groupcol, str(groupval)))
        axis.plot(group.X, group.Y, zorder=2, label=groupval, lw=2)

    # axis.legend(loc='lower right')


def parallel_coords_xy(dataframe, groupcol):
    """

    :param dataframe: data in wide format
    :param groupcol: column to use as index (e.g. playername)

    :return: column dictionary, dataframe in long format
    """

    xs = {}
    rev_xs = {}
    for col in dataframe.columns:
        if not col == groupcol:
            xs[len(xs)] = col
            rev_xs[col] = len(xs) - 1

    dataframe_long = helper.melt_helper(dataframe, id_vars=groupcol, var_name='variable', value_name='Y')
    dataframe_long.loc[:, 'X'] = dataframe_long.variable.apply(lambda x: rev_xs[x])
    return xs, dataframe_long

def add_cfpct_ref_lines_to_plot(ax, refs=None):
    """
    Adds reference lines to specified axes. For example, it could add 50%, 55%, and 45% CF% lines.

    50% has the largest width and is solid. 40%, 60%, etc will be dashed with medium width. Other numbers will be
    dotted and have the lowest width.

    Also adds little labels in center of pictured range.

    :param ax: axes. CF should be on the X axis and CA on the Y axis.
    :param refs: None, or a list of percentages (e.g. [45, 50, 55]). Defaults to every 5% from 35% to 65%

    :return: nothing
    """

    org_xlim = ax.get_xlim()
    org_ylim = ax.get_ylim()

    smaller_min = min(org_xlim[0], org_ylim[0])
    larger_max = max(org_xlim[1], org_ylim[1])

    if refs is None:
        refs = list(range(0, 101, 5))

    # Convert these percentages into ratios
    # i.e. instead of cf / (cf + ca), I want cf/ca
    # cf / (cf + ca) = ref
    # cf/ref = cf + ca
    # ca = cf/ref - cf

    def get_ca_from_cfpct(cf, cfpct):
        return cf/cfpct - cf

    for ref in refs:
        color = 'lightgray'
        if ref == 50:
            linewidth = 3
            linestyle = '-'
        elif ref % 10 == 0:
            linewidth = 2
            linestyle = '--'
        else:
            linewidth = 1
            linestyle = ':'
        ys = get_ca_from_cfpct(np.array(org_xlim), ref/100)
        ax.plot(org_xlim, ys, zorder=0.5, alpha=0.2,
                lw=linewidth, color=color, ls=linestyle)

    ax.set_xlim(*org_xlim)
    ax.set_ylim(*org_ylim)

    # For adding boxes, first find the slopes of each ref line (intercepts are zero)
    refs = list(range(0, 101, 10))
    x1 = np.array([org_xlim[0] for _ in range(len(refs))])
    x2 = np.array([org_xlim[1] for _ in range(len(refs))])
    ys = get_ca_from_cfpct(np.array(org_xlim).repeat(len(refs)).reshape((2, len(refs))), np.array(refs)/100)
    y1 = ys[0]
    y2 = ys[1]
    slopes, intercepts = get_line_slope_intercept(x1, y1, x2, y2)  # intercepts all zero, as expected

    # Next find coordinates of intersections with window edges
    leftx = np.array([org_xlim[0] for _ in range(len(refs))])
    rightx = np.array([org_xlim[1] for _ in range(len(refs))])
    bottomy = np.array([org_ylim[0] for _ in range(len(refs))])
    topy = np.array([org_ylim[1] for _ in range(len(refs))])
    lefty = slopes * leftx
    righty = slopes * rightx
    bottomx = bottomy / slopes
    topx = topy / slopes

    # Iterate through and see which sides are intersected
    bbox_props = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.5)
    for ily, iry, ibx, itx, pct in zip(lefty, righty, bottomx, topx, refs):
        # Find which sides intersect
        left = org_ylim[0] <= ily <= org_ylim[1]
        right = org_ylim[0] <= iry <= org_ylim[1]
        bottom = org_xlim[0] <= ibx <= org_xlim[1]
        top = org_xlim[0] <= itx <= org_xlim[1]

        # Continue to next iteration in loop if don't have two intersections
        if sum((left, right, top, bottom)) < 2:
            continue
        if left and right:
            midx = (org_xlim[0] + org_xlim[1]) / 2
            midy = (iry + ily) / 2
        elif left and top:
            midx = (org_xlim[0] + itx) / 2
            midy = (ily + org_ylim[1]) / 2
        elif bottom and top:
            midx = (ibx + itx) / 2
            midy = (org_ylim[0] + org_ylim[1]) / 2
        elif bottom and right:
            midx = (ibx + org_xlim[1]) / 2
            midy = (org_ylim[0] + iry) / 2
        plt.annotate('{0:d}%'.format(pct), xy=(midx, midy), ha='center', va='center', bbox=bbox_props, fontsize=6,
                     zorder=0.75)


def add_good_bad_fast_slow(margin=0.05, bottomleft='Slower', bottomright='Better', topleft='Worse', topright='Faster'):
    """
    Adds better, worse, faster, slower, to current matplotlib plot. CF60 should be on the x-axis and CA60 on the y-axis.
    Also expands figure limits by margin (default 5%). That means you should use this before using, say,
    add_cfpct_ref_lines_to_plot.

    :param margin: expand figure limits by margin. Defaults to 5%.
    :param bottomleft: label to put in bottom left corner
    :param bottomright: label to put in bottom right corner
    :param topleft: label to put in top left corner
    :param topright: label to put in top right corner

    :return: nothing
    """

    xmin, xmax = plt.gca().get_xlim()
    ymin, ymax = plt.gca().get_ylim()

    xdiff = xmax - xmin
    ydiff = ymax - ymin

    plt.gca().set_xlim(xmin - margin * xdiff, xmax + margin * xdiff)
    plt.gca().set_ylim(ymin - margin * ydiff, ymax + margin * ydiff)

    bbox_props = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)
    plt.annotate(topright, xy=(0.95, 0.95), xycoords='axes fraction', bbox=bbox_props, ha='center', va='center')
    plt.annotate(bottomleft, xy=(0.05, 0.05), xycoords='axes fraction', bbox=bbox_props, ha='center', va='center')
    plt.annotate(bottomright, xy=(0.95, 0.05), xycoords='axes fraction', bbox=bbox_props, ha='center', va='center')
    plt.annotate(topleft, xy=(0.05, 0.95), xycoords='axes fraction', bbox=bbox_props, ha='center', va='center')


def get_line_slope_intercept(x1, y1, x2, y2):
    """Returns slope and intercept of lines defined by given coordinates"""
    m = (y2 - y1) / (x2 - x1)
    b = y1 - m*x1
    return m, b
