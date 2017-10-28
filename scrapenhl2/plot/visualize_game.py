import matplotlib.pyplot as plt
import numpy as np  # standard scientific python stack
import pandas as pd  # standard scientific python stack

from scrapenhl2.manipulate import manipulate as manip
from scrapenhl2.scrape import autoupdate
from scrapenhl2.scrape import parse_pbp
from scrapenhl2.scrape import parse_toi
from scrapenhl2.scrape import schedules
from scrapenhl2.scrape import team_info


def game_timeline(season, game, save_file=None):
    """
    Creates a shot attempt timeline as seen on @muneebalamcu
    :param season: int, the season
    :param game: int, the game
    :param save_file: str, specify a valid filepath to save to file. If None, merely shows on screen. Specify 'fig'
    to return the figure
    :return: nothing
    """
    plt.clf()

    hname = team_info.team_as_str(schedules.get_home_team(season, game))
    rname = team_info.team_as_str(schedules.get_road_team(season, game))

    cf = {hname: _get_home_cf_for_timeline(season, game), rname: _get_road_cf_for_timeline(season, game)}
    pps = {hname: _get_home_adv_for_timeline(season, game), rname: _get_road_adv_for_timeline(season, game)}
    gs = {hname: _get_home_goals_for_timeline(season, game), rname: _get_road_goals_for_timeline(season, game)}
    colors = {hname: plt.rcParams['axes.prop_cycle'].by_key()['color'][0],
              rname: plt.rcParams['axes.prop_cycle'].by_key()['color'][1]}
    darkercolors = {team: _make_color_darker(hex=col) for team, col in colors.items()}

    # Corsi lines
    for team in cf:
        plt.plot(cf[team].Time, cf[team].CumCF, label=team, color=colors[team])

    # Ticks every 10 min
    plt.xticks(range(0, cf[hname].Time.max() + 1, 10))
    plt.xlabel('Time elapsed in game (min)')

    # Label goal counts when scored with diamonds
    for team in gs:
        xs, ys = _goal_times_to_scatter_for_timeline(gs[team], cf[team])
        plt.scatter(xs, ys, edgecolors='k', marker='D', label='{0:s} goal'.format(team), zorder=3, color=colors[team])

    # Bold lines to separate periods
    _, ymax = plt.ylim()
    for x in range(0, cf[hname].Time.max(), 20):
        plt.plot([x, x], [0, ymax], color='k', lw=2)

    # PP highlighting
    # Note that axvspan works in relative coords (0 to 1), so need to divide by ymax
    for team in pps:
        for pptype in pps[team]:
            if pptype[-2:] == '+1':
                colors_to_use = colors
            else:
                colors_to_use = darkercolors
            for i, (start, end) in enumerate(pps[team][pptype]):
                cf_at_time_min = cf[team].loc[cf[team].Time == start//60].CumCF.iloc[0] - 2
                if end // 60 == cf[team].Time.max():  # might happen for live games
                    cf_at_time_max = cf[team][cf[team].Time == end // 60].CumCF.iloc[0] + 2
                else:
                    cf_at_time_max = cf[team][cf[team].Time == end // 60 + 1].CumCF.iloc[0] + 2
                if i == 0:
                    plt.gca().axvspan(start / 60, end / 60, ymin=cf_at_time_min/ymax,
                                      ymax=cf_at_time_max/ymax, alpha=0.5, facecolor=colors_to_use[team],
                                      label='{0:s} {1:s}'.format(team, pptype))
                else:
                    plt.gca().axvspan(start / 60, end / 60, ymin=cf_at_time_min/ymax,
                                      ymax=cf_at_time_max/ymax, alpha=0.5, facecolor=colors[team])
                plt.gca().axvspan(start / 60, end / 60, ymin=0, ymax=0.05, alpha=0.5, facecolor=colors_to_use[team])

    # Set limits
    plt.xlim(0, cf[hname].Time.max())
    plt.ylim(0, ymax)
    plt.ylabel('Cumulative CF')
    plt.legend(loc=2, framealpha=0.5, fontsize=8)

    # Set title
    plt.title(_get_corsi_timeline_title(season, game))

    if save_file is None:
        plt.show()
    elif save_file == 'fig':
        return plt.gcf()
    else:
        plt.savefig(save_file)



def _get_home_adv_for_timeline(season, game):
    """
    Identifies times where home team had a PP or extra attacker, for highlighting on timeline
    :param season: int, the game
    :param game: int, the season
    :return: a dictionary: {'PP+1': ((start, end), (start, end), ...), 'PP+2': ((start, end), (start, end), ...)...}
    """
    # TODO add functionality for extra attacker

    toi = parse_toi.get_parsed_toi(season, game)

    pp1 = toi[((toi.HomeStrength == "5") & (toi.RoadStrength == "4")) |
              ((toi.HomeStrength == "4") & (toi.RoadStrength == "3"))].Time
    pp2 = toi[(toi.HomeStrength == "5") & (toi.RoadStrength == "3")].Time

    df = {'PP+1': _get_contiguous_times(sorted(list(pp1))),
          'PP+2': _get_contiguous_times(sorted(list(pp2)))}
    return df


def _get_road_adv_for_timeline(season, game):
    """
    Identifies times where home team had a PP or extra attacker, for highlighting on timeline
    :param season: int, the game
    :param game: int, the season
    :return: a dictionary: {'PP+1': ((start, end), (start, end), ...), 'PP+2': ((start, end), (start, end), ...)...}
    """
    # TODO add functionality for extra attacker

    toi = parse_toi.get_parsed_toi(season, game)

    pp1 = toi[((toi.HomeStrength == "4") & (toi.RoadStrength == "5")) |
              ((toi.HomeStrength == "3") & (toi.RoadStrength == "4"))].Time
    pp2 = toi[(toi.HomeStrength == "3") & (toi.RoadStrength == "5")].Time

    df = {'PP+1': _get_contiguous_times(sorted(list(pp1))),
          'PP+2': _get_contiguous_times(sorted(list(pp2)))}
    return df


def _get_contiguous_times(times, tolerance=2):
    """
    Returns tuples of start and end times inferred from list of all times.

    For example, [1, 2, 3, 5, 6, 7, 10] would yield ((1, 3), (5, 7), (10, 10))
    :param times: a list or series of ints. Must be sorted ascending.
    :param tolerance: gaps must be at least this long to be registered. E.g. 2 skips 1-sec shift anomalies
    :return: tuple of tuple-2s of ints
    """
    cont_times = []
    for i in range(len(times)):
        if i == 0:
            cont_times.append([times[i], times[i]])
        else:
            if times[i] == times[i - 1] + 1:
                cont_times[-1][-1] = times[i]
            else:
                cont_times.append([times[i], times[i]])
    cont_times = ((s, e) for s, e in cont_times if e - s >= tolerance)
    return cont_times


def _get_corsi_timeline_title(season, game):
    """
    Returns the default chart title for corsi timelines.
    :param season: int, the season
    :param game: int, the game
    :return: str, the title
    """
    otso_str = schedules.get_game_result(season, game)
    if otso_str[:2] == 'OT' or otso_str[:2] == 'SO':
        otso_str = ' ({0:s})'.format(otso_str[:2])
    else:
        otso_str = ''
    # Add strings to a list then join them together with newlines
    titletext = ('Shot attempt timeline for {0:d}-{1:s} Game {2:d} ({3:s})'.format(
        int(season), str(int(season + 1))[2:], int(game), schedules.get_game_date(season, game)),
                 '{0:s} {1:d} at {2:s} {3:d}{4:s} ({5:s})'.format(
                     team_info.team_as_str(schedules.get_road_team(season, game), abbreviation=False),
                     schedules.get_road_score(season, game),
                     team_info.team_as_str(schedules.get_home_team(season, game), abbreviation=False),
                     schedules.get_home_score(season, game),
                     otso_str, schedules.get_game_status(season, game)))

    return '\n'.join(titletext)


def _goal_times_to_scatter_for_timeline(goal_times, cfdf):
    """
    A helper methods that translates a list of goal times to coordinates for goal markers.
    Goal markers are placed on the cumulative CF line, with more 2 stacked markers if it's the 2nd goal, 3 if 3rd, etc
    :param goal_times: list of int or float
    :param cfdf: dataframe with Time and CumCF columns
    :return: (x_coords, y_coords)
    """
    goal_xs = []
    goal_ys = []
    cumgoals = 0
    for i in range(len(goal_times)):
        cumgoals += 1
        cf_at_time = cfdf[cfdf.Time - 1 <= goal_times[i]]
        cf_at_time = cf_at_time.sort_values('Time', ascending=False).CumCF.iloc[0]
        # cf_at_time = cfdf[cfdf.Time == goal_times[i]].CumCF.iloc[0]  # if I use float times need filter etc
        for j in range(cumgoals):
            goal_xs.append(goal_times[i])
            goal_ys.append(cf_at_time + j)
    return goal_xs, goal_ys


def _get_home_goals_for_timeline(season, game, granularity='min'):
    """
    Returns a list of goal times for home team
    :param season: int, the season
    :param game: int, the game
    :param granularity: can respond in minutes, or seconds, elapsed in game
    :return: a list of int, seconds elapsed
    """
    return get_goals_for_timeline(season, game, 'H', granularity)


def _get_road_goals_for_timeline(season, game, granularity='min'):
    """
    Returns a list of goal times for road team
    :param season: int, the season
    :param game: int, the game
    :param granularity: can respond in minutes, or seconds, elapsed in game
    :return: a list of int, seconds elapsed
    """
    return get_goals_for_timeline(season, game, 'R', granularity)


def get_goals_for_timeline(season, game, homeroad, granularity='min'):
    """
    Returns a list of goal times
    :param season: int, the season
    :param game: int, the game
    :param homeroad: str, 'H' for home and 'R' for road
    :param granularity: can respond in minutes, or seconds, elapsed in game
    :return: a list of int, seconds elapsed
    """

    pbp = parse_pbp.get_parsed_pbp(season, game)
    if homeroad == 'H':
        teamid = schedules.get_home_team(season, game)
    elif homeroad == 'R':
        teamid = schedules.get_road_team(season, game)
    pbp = pbp[pbp.Team == teamid]

    if granularity == 'min':
        pbp.loc[:, 'Time'] = pbp.Time / 60

    goals = pbp[pbp.Event == 'Goal'].sort_values('Time')
    return list(goals.Time)


def _get_cf_for_timeline(season, game, homeroad, granularity='min'):
    """
    Returns a dataframe with columns for time and cumulative CF
    :param season: int, the season
    :param game: int, the game
    :param homeroad: str, 'H' for home and 'R' for road
    :param granularity: can respond in minutes, or seconds, elapsed in game
    :return: a dataframe with two columns
    """

    pbp = parse_pbp.get_parsed_pbp(season, game)
    pbp = manip.filter_for_corsi(pbp)

    if homeroad == 'H':
        teamid = schedules.get_home_team(season, game)
    elif homeroad == 'R':
        teamid = schedules.get_road_team(season, game)
    pbp = pbp[pbp.Team == teamid]

    maxtime = len(parse_toi.get_parsed_toi(season, game))
    df = pd.DataFrame({'Time': list(range(maxtime))})
    df = df.merge(pbp[['Time']].assign(CF=1), how='left', on='Time')
    # df.loc[:, 'Time'] = df.Time + 1
    df.loc[:, 'CF'] = df.CF.fillna(0)
    df.loc[:, 'CumCF'] = df.CF.cumsum()

    df.drop('CF', axis=1, inplace=True)

    # Now let's shift things down. Right now a shot at 30 secs will mean Time = 0 has CumCF = 1.

    if granularity == 'min':
        df.loc[:, 'Time'] = df.Time // 60
        df = df.groupby('Time').max().reset_index()

    # I want it soccer style, so Time = 0 always has CumCF = 0, and that first shot at 30sec will register for Time=1
    df = pd.concat([pd.DataFrame({'Time': [-1], 'CumCF': [0]}), df])
    df.loc[:, 'Time'] = df.Time + 1

    return df


def _get_home_cf_for_timeline(season, game, granularity='min'):
    """
    Returns a dataframe with columns Time and cumulative CF
    :param season: int, the season
    :param game: int, the game
    :param granularity: can respond in minutes, or seconds, elapsed in game
    :return: a two-column dataframe
    """
    return _get_cf_for_timeline(season, game, 'H', granularity)


def _get_road_cf_for_timeline(season, game, granularity='min'):
    """
    Returns a dataframe with columns Time and cumulative CF
    :param season: int, the season
    :param game: int, the game
    :param granularity: can respond in minutes, or seconds, elapsed in game
    :return: a two-column dataframe
    """
    return _get_cf_for_timeline(season, game, 'R', granularity)


def game_h2h(season, game, save_file=None):
    """
    Creates the grid H2H charts seen on @muneebalamcu
    :param season: int, the season
    :param game: int, the game
    :param save_file: str, specify a valid filepath to save to file. If None, merely shows on screen.
    :return: nothing
    """
    plt.clf()
    h2htoi = manip.get_game_h2h_toi(season, game).query('Team1 == "H" & Team2 == "R"')
    h2hcorsi = manip.get_game_h2h_corsi(season, game).query('Team1 == "H" & Team2 == "R"')
    playerorder_h, numf_h = _get_h2h_chart_player_order(season, game, 'H')
    playerorder_r, numf_r = _get_h2h_chart_player_order(season, game, 'R')

    # TODO create chart and filter out RH, HH, and RR
    # TODO link players by ID. When I link by name have issue with Mike Green for example
    return _game_h2h_chart(season, game, h2hcorsi, h2htoi, playerorder_h, playerorder_r, numf_h, numf_r, save_file)


def _game_h2h_chart(season, game, corsi, toi, orderh, orderr, numf_h=None, numf_r=None, save_file=None):
    """

    :param season: int, the season
    :param game: int, the game
    :param
    :param corsi: df of P1, P2, Corsi +/- for P1
    :param toi: df of P1, P2, H2H TOI
    :param orderh: list of float, player order on y-axis, top to bottom
    :param orderr: list of float, player order on x-axis, left to right
    :param numf_h: int. Number of forwards for home team. Used to add horizontal bold line between F and D
    :param numf_r: int. Number of forwards for road team. Used to add vertical bold line between F and D.
    :param save_file: str of file to save the figure to, or None to simply display
    :return: nothing
    """

    hname = team_info.team_as_str(schedules.get_home_team(season, game), True)
    homename = team_info.team_as_str(schedules.get_home_team(season, game), False)
    rname = team_info.team_as_str(schedules.get_road_team(season, game), True)
    roadname = team_info.team_as_str(schedules.get_road_team(season, game), False)

    fig, ax = plt.subplots(1, figsize=[11, 7])

    # Convert dataframes to coordinates
    horderdf = pd.DataFrame({'PlayerID1': orderh[::-1], 'Y': list(range(len(orderh)))})
    rorderdf = pd.DataFrame({'PlayerID2': orderr, 'X': list(range(len(orderr)))})
    plotdf = toi.merge(corsi, how='left', on=['PlayerID1', 'PlayerID2']) \
        .merge(horderdf, how='left', on='PlayerID1') \
        .merge(rorderdf, how='left', on='PlayerID2')

    # Hist2D of TOI
    # I make the bins a little weird so my coordinates are centered in them. Otherwise, they're all on the edges.
    _, _, _, image = ax.hist2d(x=plotdf.X, y=plotdf.Y, bins=(np.arange(-0.5, len(orderr) + 0.5, 1),
                                                             np.arange(-0.5, len(orderh) + 0.5, 1)),
                               weights=plotdf.Min, cmap=plt.cm.summer)

    # Convert IDs to names and label axes and axes ticks
    ax.set_xlabel(roadname)
    ax.set_ylabel(homename)
    xorder = players.playerlst_as_str(orderr)
    yorder = players.playerlst_as_str(orderh)[::-1]  # need to go top to bottom, so reverse order
    ax.set_xticks(range(len(xorder)))
    ax.set_yticks(range(len(yorder)))
    ax.set_xticklabels(xorder, fontsize=10, rotation=45, ha='right')
    ax.set_yticklabels(yorder, fontsize=10)
    ax.set_xlim(-0.5, len(orderr) - 0.5)
    ax.set_ylim(-0.5, len(orderh) - 0.5)

    # Hide the little ticks on the axes by setting their length to 0
    ax.tick_params(axis='both', which='both', length=0)

    # Add dividing lines between rows
    for x in np.arange(0.5, len(orderr) - 0.5, 1):
        ax.plot([x, x], [-0.5, len(orderh) - 0.5], color='k')
    for y in np.arange(0.5, len(orderh) - 0.5, 1):
        ax.plot([-0.5, len(orderr) - 0.5], [y, y], color='k')

    # Add a bold line between F and D.
    if numf_r is not None:
        ax.plot([numf_r - 0.5, numf_r - 0.5], [-0.5, len(orderh) - 0.5], color='k', lw=3)
    if numf_h is not None:
        ax.plot([-0.5, len(orderr) - 0.5], [len(orderh) - numf_h - 0.5, len(orderh) - numf_h - 0.5], color='k', lw=3)

    # Colorbar for TOI
    cbar = fig.colorbar(image, pad=0.1)
    cbar.ax.set_ylabel('TOI (min)')

    # Add trademark
    cbar.ax.set_xlabel('Muneeb Alam\n@muneebalamcu', labelpad=20)

    # Add labels for Corsi and circle negatives
    neg_x = []
    neg_y = []
    for y in range(len(orderh)):
        hpid = orderh[len(orderh) - y - 1]
        for x in range(len(orderr)):
            rpid = orderr[x]

            cf = corsi[(corsi.PlayerID1 == hpid) & (corsi.PlayerID2 == rpid)]
            if len(cf) == 0:  # In this case, player will not have been on ice for a corsi event
                cf = 0
            else:
                cf = int(cf.HomeCorsi.iloc[0])

            if cf == 0:
                cf = '0'
            elif cf > 0:
                cf = '+' + str(cf)  # Easier to pick out positives with plus sign
            else:
                cf = str(cf)
                neg_x.append(x)
                neg_y.append(y)

            ax.annotate(cf, xy=(x, y), ha='center', va='center')

    # Circle negative numbers by making a scatterplot with black edges and transparent faces
    ax.scatter(neg_x, neg_y, marker='o', edgecolors='k', s=200, facecolors='none')

    # Add TOI and Corsi totals at end of rows/columns
    topax = ax.twiny()
    topax.set_xticks(range(len(xorder)))
    rtotals = pd.DataFrame({'PlayerID2': orderr}) \
        .merge(toi[['PlayerID2', 'Secs']].groupby('PlayerID2').sum().reset_index(),
               how='left', on='PlayerID2') \
        .merge(corsi[['PlayerID2', 'HomeCorsi']].groupby('PlayerID2').sum().reset_index(),
               how='left', on='PlayerID2')
    rtotals.loc[:, 'HomeCorsi'] = rtotals.HomeCorsi.fillna(0)
    rtotals.loc[:, 'CorsiLabel'] = rtotals.HomeCorsi.apply(lambda x: _format_number_with_plus(-1 * int(x / 5)))
    rtotals.loc[:, 'TOILabel'] = rtotals.Secs.apply(lambda x: manip.time_to_mss(x / 5))
    toplabels = ['{0:s} in {1:s}'.format(x, y) for x, y, in zip(list(rtotals.CorsiLabel), list(rtotals.TOILabel))]

    ax.set_xticks(range(len(xorder)))
    topax.set_xticklabels(toplabels, fontsize=6, rotation=45, ha='left')
    topax.set_xlim(-0.5, len(orderr) - 0.5)
    topax.tick_params(axis='both', which='both', length=0)

    rightax = ax.twinx()
    rightax.set_yticks(range(len(yorder)))
    htotals = pd.DataFrame({'PlayerID1': orderh[::-1]}) \
        .merge(toi[['PlayerID1', 'Secs']].groupby('PlayerID1').sum().reset_index(),
               how='left', on='PlayerID1') \
        .merge(corsi[['PlayerID1', 'HomeCorsi']].groupby('PlayerID1').sum().reset_index(),
               how='left', on='PlayerID1')
    htotals.loc[:, 'HomeCorsi'] = htotals.HomeCorsi.fillna(0)
    htotals.loc[:, 'CorsiLabel'] = htotals.HomeCorsi.apply(lambda x: _format_number_with_plus(int(x / 5)))
    htotals.loc[:, 'TOILabel'] = htotals.Secs.apply(lambda x: manip.time_to_mss(x / 5))
    rightlabels = ['{0:s} in {1:s}'.format(x, y) for x, y, in zip(list(htotals.CorsiLabel), list(htotals.TOILabel))]

    rightax.set_yticks(range(len(yorder)))
    rightax.set_yticklabels(rightlabels, fontsize=6)
    rightax.set_ylim(-0.5, len(orderh) - 0.5)
    rightax.tick_params(axis='both', which='both', length=0)

    # plt.subplots_adjust(top=0.80)
    # topax.set_ylim(-0.5, len(orderh) - 0.5)

    # Add brief explanation for the top left cell at the bottom
    explanation = []
    row1name = yorder.iloc[-1]
    col1name = xorder.iloc[0]
    timeh2h = int(toi[(toi.PlayerID1 == orderh[0]) & (toi.PlayerID2 == orderr[0])].Secs.iloc[0])
    shoth2h = int(corsi[(corsi.PlayerID1 == orderh[0]) & (corsi.PlayerID2 == orderr[0])].HomeCorsi.iloc[0])

    explanation.append('The top left cell indicates {0:s} (row 1) faced {1:s} (column 1) for {2:s}.'.format(
        row1name, col1name, manip.time_to_mss(timeh2h)))
    if shoth2h == 0:
        explanation.append('During that time, {0:s} and {1:s} were even in attempts.'.format(hname, rname))
    elif shoth2h > 0:
        explanation.append('During that time, {0:s} out-attempted {1:s} by {2:d}.'.format(hname, rname, shoth2h))
    else:
        explanation.append('During that time, {1:s} out-attempted {0:s} by {2:d}.'.format(hname, rname, -1 * shoth2h))
    explanation = '\n'.join(explanation)

    # Hacky way to annotate: add this to x-axis label
    ax.set_xlabel(ax.get_xlabel() + '\n\n' + explanation)

    plt.subplots_adjust(bottom=0.27)
    plt.subplots_adjust(left=0.17)
    plt.subplots_adjust(top=0.82)
    plt.subplots_adjust(right=1.0)

    # Add title
    plt.title(_get_game_h2h_chart_title(season, game, corsi.HomeCorsi.sum() / 25, toi.Secs.sum() / 25),
              y=1.1, va='bottom')

    # fig.tight_layout()
    if save_file is None:
        plt.show()
    elif save_file == 'fig':
        return plt.gcf()
    else:
        plt.savefig(save_file)


def _get_game_h2h_chart_title(season, game, homecf_diff=None, totaltoi=None):
    """
    Returns the title for the H2H chart
    :param season: int, the season
    :param game: int, the game
    :param homecf_diff: int. The home team corsi advantage
    :param totaltoi: int. The TOI played so far.
    :return:
    """
    titletext = []
    # Note if a game was OT or SO
    otso_str = schedules.get_game_result(season, game)
    if otso_str[:2] == 'OT' or otso_str[:2] == 'SO':
        otso_str = ' ({0:s})'.format(otso_str[:2])
    else:
        otso_str = ''
    # Add strings to a list then join them together with newlines
    titletext.append('H2H Corsi and TOI for {0:d}-{1:s} Game {2:d}'.format(season, str(season + 1)[2:], game))
    titletext.append('{0:s} {1:d} at {2:s} {3:d}{4:s} ({5:s})'.format(
        team_info.team_as_str(schedules.get_road_team(season, game), abbreviation=False),
        schedules.get_road_score(season, game),
        team_info.team_as_str(schedules.get_home_team(season, game), abbreviation=False),
        schedules.get_home_score(season, game),
        otso_str, schedules.get_game_status(season, game)))
    if homecf_diff is not None and totaltoi is not None:
        titletext.append('{0:s} {1:s} in 5v5 attempts in {2:s}'.format(
            team_info.team_as_str(schedules.get_home_team(season, game)),
            _format_number_with_plus(int(homecf_diff)), manip.time_to_mss(int(totaltoi))))
    return '\n'.join(titletext)


def _get_h2h_chart_player_order(season, game, homeroad='H'):
    """
    Reads lines and pairs for this game and finds arrangement using this algorithm:

    - Top player in TOI
    - First player's top line combination, player with more total TOI
    - First player's top line combination, player with less total TOI
    - Top player in TOI not already listed
    - (etc)
    :param season: int, the game
    :param game: int, the season
    :param homeroad: str, 'H' for home or 'R' for road
    :return: [list of IDs], NumFs
    """
    combos = manip.get_line_combos(season, game, homeroad)
    pairs = manip.get_pairings(season, game, homeroad)

    playerlist = []

    # forwards
    # I can simply drop PlayerID2 because dataframe contains duplicates of every line
    ftoi = manip.get_player_toi(season, game, 'F', homeroad)
    while len(ftoi) > 0:
        next_player = ftoi.PlayerID.iloc[0]
        top_line_for_next_player = combos[(combos.PlayerID1 == next_player) | (combos.PlayerID2 == next_player) |
                                          (combos.PlayerID3 == next_player)].sort_values(by='Secs', ascending=False)
        if len(top_line_for_next_player) == 0:  # sometimes this happens. Special case
            playerlist.append(next_player)
            ftoi = ftoi[ftoi.PlayerID != next_player]
            combos = combos[(combos.PlayerID1 != next_player) & (combos.PlayerID2 != next_player) &
                            (combos.PlayerID3 != next_player)]
        else:
            thisline = [top_line_for_next_player.PlayerID1.iloc[0],
                        top_line_for_next_player.PlayerID2.iloc[0],
                        top_line_for_next_player.PlayerID3.iloc[0]]
            thislinedf = ftoi[(ftoi.PlayerID == thisline[0]) | (ftoi.PlayerID == thisline[1]) |
                              (ftoi.PlayerID == thisline[2])].sort_values(by='Secs', ascending=False)

            playerlist += list(thislinedf.PlayerID.values)

            # Remove these players from ftoi
            ftoi = ftoi.merge(thislinedf[['PlayerID']], how='outer', indicator=True) \
                .query('_merge == "left_only"') \
                .drop('_merge', axis=1)
            # Remove these players from combos df
            for i in range(3):
                combos = combos[(combos.PlayerID1 != thisline[i]) & (combos.PlayerID2 != thisline[i]) &
                                (combos.PlayerID3 != thisline[i])]

    numf = len(playerlist)

    # defensemen
    dtoi = manip.get_player_toi(season, game, 'D', homeroad)
    while len(dtoi) > 0:
        next_player = dtoi.PlayerID.iloc[0]
        top_line_for_next_player = pairs[(pairs.PlayerID1 == next_player) | (pairs.PlayerID2 == next_player)] \
            .sort_values(by='Secs', ascending=False)
        if len(top_line_for_next_player) == 0:
            playerlist.append(next_player)
            dtoi = dtoi[dtoi.PlayerID != next_player]
            pairs = pairs[(pairs.PlayerID1 != next_player) & (pairs.PlayerID2 != next_player)]
        else:
            thispair = [top_line_for_next_player.PlayerID1.iloc[0],
                        top_line_for_next_player.PlayerID2.iloc[0]]
            thispairdf = dtoi[(dtoi.PlayerID == thispair[0]) | (dtoi.PlayerID == thispair[1])] \
                .sort_values(by='Secs', ascending=False)

            playerlist += list(thispairdf.PlayerID.values)

            # Remove these players from dtoi
            dtoi = dtoi.merge(thispairdf[['PlayerID']], how='outer', indicator=True) \
                .query('_merge == "left_only"') \
                .drop('_merge', axis=1)
            # Remove pairs including these players from pairs df
            for i in range(2):
                pairs = pairs[(pairs.PlayerID1 != thispair[i]) & (pairs.PlayerID2 != thispair[i])]

    return playerlist, numf


def _format_number_with_plus(stringnum):
    """
    Converts 0 to 0, -1, to -1, and 1 to +1 (for presentation purposes).
    :param stringnum: int
    :return: str, transformed as specified above.
    """
    if stringnum <= 0:
        return str(stringnum)
    else:
        return '+' + str(stringnum)


def _hex_to_rgb(value):
    """Return (red, green, blue) for the color given as #rrggbb."""
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))


def _rgb_to_hex(red, green, blue):
    """Return color as #rrggbb for the given color values."""
    return '#%02x%02x%02x' % (int(red), int(green), int(blue))


def _make_color_darker(hex=None, rgb=None, returntype='hex'):
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
        color = _hex_to_rgb(hex)
    else:
        color = rgb

    color = [x * 0.5 for x in color]

    if returntype == 'rgb':
        return color
    return _rgb_to_hex(*color)


def _make_color_lighter(hex=None, rgb=None, returntype='hex'):
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
        color = _hex_to_rgb(hex)
    else:
        color = rgb

    color = [255 - ((255 - x) * 0.5) for x in color]

    if returntype == 'rgb':
        return color
    return _rgb_to_hex(*color)


if __name__ == '__main__':
    autoupdate.autoupdate()
    game_timeline(2017, 20155)
