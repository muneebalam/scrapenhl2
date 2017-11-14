"""
This module contains methods for making a stacked bar graph indicating how much TOI each team spends in score states.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

import scrapenhl2.manipulate.manipulate as manip
import scrapenhl2.scrape.team_info as team_info
import scrapenhl2.plot.visualization_helper as vhelper

def score_state_graph(season):
    """
    Generates a horizontal stacked bar graph showing how much 5v5 TOI each team has played in each score state
    for given season.

    :param season: int, the season

    :return:
    """
    #TODO make kwargs match other methods: startseason, startdate, etc

    state_toi = manip.team_5v5_score_state_summary_by_game(season) \
        .drop('Game', axis=1) \
        .groupby(['Team', 'ScoreState'], as_index=False).sum()

    bar_positions = _score_state_graph_bar_positions(state_toi)
    bar_positions.loc[:, 'Team'] = bar_positions.Team.apply(lambda x: team_info.team_as_str(x))

    plt.clf()
    tiedcolor, leadcolor, trailcolor = plt.rcParams['axes.prop_cycle'].by_key()['color'][:3]
    colors = {0: tiedcolor, 1: leadcolor, -1: trailcolor}
    for i in range(2, 4):
        colors[i] = vhelper.make_color_lighter(colors[i - 1])
        colors[-1 * i] = vhelper.make_color_lighter(colors[-1 * i + 1])
    for score in (-3, -2, -1, 0, 1, 2, 3):  # bar_positions.ScoreState.unique():
        score = int(score)
        if score == 3:
            label = 'Up 3+'
        elif score > 0:
            label = 'Up {0:d}'.format(score)
        elif score == 0:
            label = 'Tied'
        elif score == -3:
            label = 'Trail 3+'
        else:
            label = 'Trail {0:d}'.format(-1 * score)

        temp = bar_positions.query('ScoreState == {0:d}'.format(score))
        alpha = 0.5
        plt.barh(bottom=temp.Y.values, width=temp.Width.values, left=temp.Left.values, label=label, alpha=alpha,
                 color=colors[score])

    for index, y, team in bar_positions[['Y', 'Team']].drop_duplicates().itertuples():
        plt.annotate(team, xy=(0, y), ha='center', va='center', fontsize=6)

    plt.ylim(-1, len(bar_positions.Team.unique()))
    plt.legend(loc='lower right', fontsize=8)
    plt.yticks([])
    for spine in ['right', 'left', 'top', 'bottom']:
        plt.gca().spines[spine].set_visible(False)
    plt.title(get_score_state_graph_title(season))

    lst = list(np.arange(-0.6, 0.61, 0.2))
    plt.xticks(lst, ['{0:d}%'.format(abs(int(round(100 * x)))) for x in lst])
    plt.show()


def _order_for_score_state_graph(toidf):
    """
    Want to arrange teams so top team has most time leading minus trailing.

    This method sums over lead/trail, sorts, and arranges so the team with the largest (lead-trail) has the highest Y.

    :param toidf: dataframe, unique on team and score state

    :return: dataframe with team and Y
    """
    temp = toidf.assign(LeadTrail=toidf.ScoreState.apply(lambda x: 'Lead' if x > 0 else 'Trail')) \
        .query("ScoreState != 0") \
        [['Team', 'LeadTrail', 'Secs']] \
        .groupby(['Team', 'LeadTrail'], as_index=False) \
        .sum() \
        .pivot(index='Team', columns='LeadTrail', values='Secs') \
        .reset_index()
    temp = temp.assign(Diff=temp.Lead - temp.Trail).sort_values('Diff').assign(Y=1)
    temp.loc[:, 'Y'] = temp.Y.cumsum() - 1
    return temp[['Team', 'Y']]


def _score_state_graph_bar_positions(toidf):
    """
    Figures out where bars should start and stop so that the y-axis bisects the "tied" bar.

    :param toidf:

    :return:
    """

    totaltoi = toidf[['Team', 'Secs']].groupby('Team', as_index=False).sum().rename(columns={'Secs': 'TotalTOI'})

    # Trim score states to -3 to 3
    toidf.loc[:, 'ScoreState'] = toidf.ScoreState.apply(lambda x: max(-3, min(3, x)))
    toidf = toidf.groupby(['Team', 'ScoreState'], as_index=False).sum()

    # Change numbers to fractions of 100%
    df = toidf.merge(totaltoi, how='left', on='Team')
    df = df.assign(FracTOI=df.Secs / df.TotalTOI) \
        .drop({'Secs', 'TotalTOI'}, axis=1) \
        .rename(columns={'FracTOI': 'Width'}) \
        .sort_values('ScoreState')

    # Take cumsums for the left in a barh
    df.loc[:, 'Left'] = df[['Team', 'Width']].groupby('Team', as_index=False).cumsum().Width
    df.loc[:, 'Left'] = df.Left - df.Width  # because cumsum is inclusive, no remove it

    # Shift them over so the center of the tied bar is at zero
    zeroes = df.query('ScoreState == 0')
    zeroes = zeroes.assign(Shift=zeroes.Left + zeroes.Width / 2)[['Team', 'Shift']]

    # Shift
    df = df.merge(zeroes, how='left', on='Team')
    df.loc[:, 'Left'] = df.Left - df.Shift
    df = df.drop('Shift', axis=1)

    # Check that zeroes are centered
    tempdf = df.query('ScoreState == 0')
    tempdf = tempdf.assign(Diff=tempdf.Left * 2 + tempdf.Width)
    assert np.isclose(0, tempdf.Diff.sum())  # sometimes have little float nonzeroes, like 1e-16

    return df.merge(_order_for_score_state_graph(toidf), how='left', on='Team').sort_values('Y')


def get_score_state_graph_title(season):
    """

    :param season: int, the season

    :return:
    """
    return 'Team 5v5 TOI by score state in {0:d}-{1:s}'.format(season, str(season + 1)[2:])