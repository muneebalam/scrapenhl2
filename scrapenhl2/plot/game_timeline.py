"""
This module has methods for creating a game corsi timeline.
"""

import matplotlib.pyplot as plt
import pandas as pd  # standard scientific python stack

from scrapenhl2.manipulate import manipulate as manip
from scrapenhl2.plot import visualization_helper
from scrapenhl2.scrape import parse_pbp, parse_toi, schedules, team_info


def live_timeline(team1, team2, update=True, save_file=None):
    """
    A convenience method that updates data then displays timeline for most recent game between specified tams.

    :param team1: str or int, team
    :param team2: str or int, other team
    :param update: bool, should data be updated first?
    :param save_file: str, specify a valid filepath to save to file. If None, merely shows on screen.

    :return: nothing
    """
    if update:
        from scrapenhl2.scrape import autoupdate
        autoupdate.autoupdate()
    from scrapenhl2.scrape import games
    game = games.most_recent_game_id(team1, team2)
    return game_timeline(2017, game)


def game_timeline(season, game, save_file=None):
    """
    Creates a shot attempt timeline as seen on @muneebalamcu

    :param season: int, the season
    :param game: int, the game
    :param save_file: str, specify a valid filepath to save to file. If None, merely shows on screen.
        Specify 'fig' to return the figure

    :return: nothing, or the figure
    """
    plt.clf()

    hname = team_info.team_as_str(schedules.get_home_team(season, game))
    rname = team_info.team_as_str(schedules.get_road_team(season, game))

    cf = {hname: _get_home_cf_for_timeline(season, game), rname: _get_road_cf_for_timeline(season, game)}
    pps = {hname: _get_home_adv_for_timeline(season, game), rname: _get_road_adv_for_timeline(season, game)}
    gs = {hname: _get_home_goals_for_timeline(season, game), rname: _get_road_goals_for_timeline(season, game)}
    colors = {hname: plt.rcParams['axes.prop_cycle'].by_key()['color'][0],
              rname: plt.rcParams['axes.prop_cycle'].by_key()['color'][1]}
    darkercolors = {team: visualization_helper.make_color_darker(hex=col) for team, col in colors.items()}

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
                cf_at_time_min = cf[team].loc[cf[team].Time == start // 60].CumCF.iloc[0] - 2
                if end // 60 == cf[team].Time.max():  # might happen for live games
                    cf_at_time_max = cf[team][cf[team].Time == end // 60].CumCF.iloc[0] + 2
                else:
                    cf_at_time_max = cf[team][cf[team].Time == end // 60 + 1].CumCF.iloc[0] + 2
                if i == 0:
                    plt.gca().axvspan(start / 60, end / 60, ymin=cf_at_time_min / ymax,
                                      ymax=cf_at_time_max / ymax, alpha=0.5, facecolor=colors_to_use[team],
                                      label='{0:s} {1:s}'.format(team, pptype))
                else:
                    plt.gca().axvspan(start / 60, end / 60, ymin=cf_at_time_min / ymax,
                                      ymax=cf_at_time_max / ymax, alpha=0.5, facecolor=colors[team])
                plt.gca().axvspan(start / 60, end / 60, ymin=0, ymax=0.05, alpha=0.5, facecolor=colors_to_use[team])

    # Set limits
    plt.xlim(0, cf[hname].Time.max())
    plt.ylim(0, ymax)
    plt.ylabel('Cumulative CF')
    plt.legend(loc=2, framealpha=0.5, fontsize=8)

    # Set title
    plt.title(_get_corsi_timeline_title(season, game))

    plt.gcf().canvas.set_window_title('{0:d} {1:d} TL.png'.format(season, game))

    if save_file is None:
        plt.show()
    elif save_file == 'fig':
        return plt.gcf()
    else:
        plt.savefig(save_file)
    plt.close()


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

    :return: a dictionary, {'PP+1': ((start, end), (start, end), ...), 'PP+2': ((start, end), (start, end), ...)...}
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
    cont_times = tuple((s, e) for s, e in cont_times if e - s >= tolerance)
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
