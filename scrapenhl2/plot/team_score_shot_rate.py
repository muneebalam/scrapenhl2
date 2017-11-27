"""
This module creates a scatterplot for specified team with shot attempt rates versus league median from down 3 to up 3.
"""

import matplotlib.pyplot as plt
import math
import pandas as pd

import scrapenhl2.scrape.team_info as team_info
import scrapenhl2.manipulate.manipulate as manip
import scrapenhl2.plot.visualization_helper as vhelper

def team_score_shot_rate_parallel(team, startseason, endseason=None, save_file=None):
    """

    :param team:
    :param startseason:
    :param endseason:
    :param save_file:
    :return:
    """
    if endseason is None:
        endseason = startseason

    df = pd.concat([manip.team_5v5_shot_rates_by_score(season) for season in range(startseason, endseason + 1)])

    df.loc[:, 'ScoreState'] = df.ScoreState.apply(lambda x: max(min(3, x), -3))  # reduce to +/- 3
    df = df.drop('Game', axis=1) \
        .groupby(['Team', 'ScoreState'], as_index=False) \
        .sum()
    df.loc[:, 'CF%'] = df.CF / (df.CF + df.CA)
    df = df[['Team', 'ScoreState', 'CF%']] \
        .sort_values('ScoreState')

    statelabels = {x: 'Lead{0:d}'.format(x) if x >= 1 else 'Trail{0:d}'.format(abs(x)) for x in range(-3, 4)}
    statelabels[0] = 'Tied'
    df.loc[:, 'ScoreState'] = df.ScoreState.apply(lambda x: statelabels[x])

    # Go to wide
    df = df.pivot_table(index='Team', columns='ScoreState', values='CF%').reset_index()

    # Reorder columns
    df = df[['Team', 'Trail3', 'Trail2', 'Trail1', 'Tied', 'Lead1', 'Lead2', 'Lead3']]

    # Teams to strings
    df.loc[:, 'Team'] = df.Team.apply(lambda x: team_info.team_as_str(x))

    # filter for own team
    teamdf = df.query('Team == "{0:s}"'.format(team_info.team_as_str(team)))

    # Make parallel coords
    vhelper.parallel_coords(df, teamdf, 'Team')

    # Set yticklabels
    ys = (0.4, 0.5, 0.6)
    plt.yticks(ys, ['{0:d}%'.format(int(y * 100)) for y in ys])
    plt.ylim(0.35, 0.65)

    plt.title(_team_score_shot_rate_parallel_title(team, startseason, endseason))

    for direction in ['right', 'top', 'bottom', 'left']:
        plt.gca().spines[direction].set_visible(False)

    if save_file is None:
        plt.show()
    else:
        plt.savefig(save_file)


def team_score_shot_rate_scatter(team, startseason, endseason=None, save_file=None):
    """

    :param team: str or int, team
    :param startseason: int, the starting season (inclusive)
    :param endseason: int, the ending season (inclusive)

    :return: nothing
    """

    if endseason is None:
        endseason = startseason

    df = pd.concat([manip.team_5v5_shot_rates_by_score(season) for season in range(startseason, endseason + 1)])

    df.loc[:, 'ScoreState'] = df.ScoreState.apply(lambda x: max(min(3, x), -3))  # reduce to +/- 3
    df = df.drop('Game', axis=1) \
        .groupby(['Team', 'ScoreState'], as_index=False) \
        .sum()
    df.loc[:, 'CF60'] = df.CF * 3600 / df.Secs
    df.loc[:, 'CA60'] = df.CA * 3600 / df.Secs

    # get medians
    medians = df[['ScoreState', 'CF60', 'CA60', 'Secs']].groupby('ScoreState', as_index=False).median()

    # filter for own team
    teamdf = df.query('Team == {0:d}'.format(int(team_info.team_as_id(team))))

    statelabels = {x: 'Lead {0:d}'.format(x) if x >= 1 else 'Trail {0:d}'.format(abs(x)) for x in range(-3, 4)}
    statelabels[0] = 'Tied'
    for state in range(-3, 4):
        teamxy = teamdf.query('ScoreState == {0:d}'.format(state))
        teamx = teamxy.CF60.iloc[0]
        teamy = teamxy.CA60.iloc[0]

        leaguexy = medians.query('ScoreState == {0:d}'.format(state))
        leaguex = leaguexy.CF60.iloc[0]
        leaguey = leaguexy.CA60.iloc[0]

        midx = (leaguex + teamx) / 2
        midy = (leaguey + teamy) / 2

        rot = _calculate_label_rotation(leaguex, leaguey, teamx, teamy)

        plt.annotate('', xy=(teamx, teamy), xytext=(leaguex, leaguey), xycoords='data',
                     arrowprops={'arrowstyle': '-|>'})
        plt.annotate(statelabels[state], xy=(midx, midy), ha="center", va="center", xycoords='data', size=8,
                     rotation=rot, bbox=dict(boxstyle="round", fc="w", alpha=0.9))

    plt.scatter(medians.CF60.values, medians.CA60.values, s=100, color='w')
    plt.scatter(teamdf.CF60.values, teamdf.CA60.values, s=100, color='w')

    #bbox_props = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)
    #plt.annotate('Fast', xy=(0.95, 0.95), xycoords='axes fraction', bbox=bbox_props, ha='center', va='center')
    #plt.annotate('Slow', xy=(0.05, 0.05), xycoords='axes fraction', bbox=bbox_props, ha='center', va='center')
    #plt.annotate('Good', xy=(0.95, 0.05), xycoords='axes fraction', bbox=bbox_props, ha='center', va='center')
    #plt.annotate('Bad', xy=(0.05, 0.95), xycoords='axes fraction', bbox=bbox_props, ha='center', va='center')
    vhelper.add_good_bad_fast_slow()

    plt.xlabel('CF60')
    plt.ylabel('CA60')

    plt.title(_team_score_shot_rate_scatter_title(team, startseason, endseason))

    if save_file is None:
        plt.show()
    else:
        plt.savefig(save_file)


def _team_score_shot_rate_scatter_title(team, startseason, endseason):
    """

    :param team:
    :param startseason:
    :param endseason:
    :return:
    """
    return '{0:s} shot rate by score state, {1:s} to {2:s}'.format(team_info.team_as_str(team),
                                                                   *vhelper.get_startdate_enddate_from_kwargs(
                                                                       startseason=startseason,
                                                                       endseason=endseason))


def _team_score_shot_rate_parallel_title(team, startseason, endseason):
    """

    :param team:
    :param startseason:
    :param endseason:
    :return:
    """
    return '{0:s} CF% by score state\n{1:s} to {2:s}'.format(team_info.team_as_str(team),
                                                             *vhelper.get_startdate_enddate_from_kwargs(
                                                                 startseason=startseason,
                                                                 endseason=endseason))

def _calculate_label_rotation(startx, starty, endx, endy):
    """
    Calculates the appropriate rotation angle for a label on an arrow (matches line, is between -90 and 90 degrees)

    :param startx: start of arrow (x)
    :param starty: start of arrow (y)
    :param endx: end of arrow (x)
    :param endy: end of arrow (y)

    :return: rotation angle.
    """
    return math.degrees(math.atan((endy - starty)/(endx - startx)))
