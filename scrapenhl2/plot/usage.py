"""
This module creates static and animated usage charts.
"""

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd
import numpy as np

from scrapenhl2.scrape import players, team_info, schedules
from scrapenhl2.scrape import general_helpers as helpers
from scrapenhl2.plot import visualization_helper as vhelper
from scrapenhl2.plot import label_lines

def parallel_coords_team_comparison(**kwargs):
    """

    :param kwargs:

    :return: nothing, or figure
    """
    if 'startdate' not in kwargs and 'enddate' not in kwargs and \
                    'startseason' not in kwargs and 'endseason' not in kwargs:
        kwargs['last_n_days'] = 30

    qocqot = vhelper.get_and_filter_5v5_log(**kwargs)
    qocqot = qocqot[['PlayerID', 'TOION', 'TOIOFF',
                     'FCompSum', 'FCompN', 'DCompSum', 'DCompN',
                     'FTeamSum', 'FTeamN', 'DTeamSum', 'DTeamN', 'Team']] \
        .groupby(['PlayerID', 'Team']).sum().reset_index()
    qocqot.loc[:, 'FQoC'] = qocqot.FCompSum / qocqot.FCompN
    qocqot.loc[:, 'FQoT'] = qocqot.FTeamSum / qocqot.FTeamN
    qocqot.loc[:, 'DQoC'] = qocqot.DCompSum / qocqot.DCompN
    qocqot.loc[:, 'DQoT'] = qocqot.DTeamSum / qocqot.DTeamN
    qocqot.loc[:, 'TOI60'] = qocqot.TOION / (qocqot.TOION + qocqot.TOIOFF)
    qocqot = qocqot.dropna().sort_values('TOI60', ascending=False)  # In case I have zeroes

    qocqot.loc[:, 'PlayerName'] = qocqot.PlayerID.apply(lambda x: helpers.get_lastname(players.player_as_str(x)))
    qocqot.loc[:, 'PlayerInitials'] = qocqot.PlayerID.apply(lambda x: helpers.get_lastname(players.player_as_str(x)))
    qocqot.loc[:, 'Position'] = qocqot.PlayerID.apply(lambda x: players.get_player_position(x))
    qocqot.drop({'FCompSum', 'FCompN', 'DCompSum', 'DCompN', 'FTeamSum', 'FTeamN', 'DTeamSum', 'DTeamN',
                 'PlayerID'}, axis=1, inplace=True)

    # Reorder columns for the parallel coordinates plot
    qocqot = qocqot[['FQoT', 'FQoC', 'DQoC', 'DQoT', 'TOION', 'TOIOFF', 'TOI60', 'PlayerName', 'PlayerInitials',
                     'Position']] \
        .sort_values('TOION', ascending=False) \
        .drop({'TOION', 'TOION', 'TOIOFF', 'TOI60'}, axis=1)

    fig, axes = plt.subplots(2, 2, sharex=True, sharey=True, figsize=[11, 7])

    forwards = qocqot.query('Position != "D"')
    centers = forwards.query('Position == "C"').drop('Position', axis=1).iloc[:6, :]
    wingers = forwards.query('Position != "C"').drop('Position', axis=1).iloc[:6, :]
    forwards.drop('Position', axis=1, inplace=True)
    vhelper.parallel_coords(forwards, centers, 'PlayerInitials', 'PlayerName', axes.flatten()[0])
    vhelper.parallel_coords(forwards, wingers, 'PlayerInitials', 'PlayerName', axes.flatten()[1])

    alldefense = qocqot.query('Position == "D"').drop('Position', axis=1)
    defense = alldefense.iloc[:6, :]
    vhelper.parallel_coords(alldefense, defense, 'PlayerInitials', 'PlayerName', axes.flatten()[2])

    other_players = pd.concat([qocqot.drop('Position', axis=1), centers, wingers, defense]) \
        .drop_duplicates(keep=False).iloc[:6, :]
    vhelper.parallel_coords(pd.concat([forwards, defense]), other_players, 'PlayerInitials', 'PlayerName', axes.flatten()[3])

    fig.text(0.5, 0.04, 'Statistic (based on TOI/60)', ha='center')
    fig.text(0.04, 0.5, 'Minutes', va='center', rotation='vertical')
    axes.flatten()[0].set_title('Top centers')
    axes.flatten()[1].set_title('Top wingers')
    axes.flatten()[2].set_title('Top defense')
    axes.flatten()[3].set_title('Others')

    fig.suptitle(_parallel_usage_chart_title(**kwargs))

    return vhelper.savefilehelper(**kwargs)


def parallel_usage_chart(**kwargs):
    """

    :param kwargs: Defaults to take last month of games for all teams.

    :return: nothing, or figure
    """
    if 'startdate' not in kwargs and 'enddate' not in kwargs and \
                    'startseason' not in kwargs and 'endseason' not in kwargs:
        kwargs['last_n_days'] = 30

    qocqot = vhelper.get_and_filter_5v5_log(**kwargs)
    qocqot = qocqot[['PlayerID', 'TOION', 'TOIOFF',
                     'FCompSum', 'FCompN', 'DCompSum', 'DCompN',
                     'FTeamSum', 'FTeamN', 'DTeamSum', 'DTeamN']] \
        .groupby('PlayerID').sum().reset_index()
    qocqot.loc[:, 'FQoC'] = qocqot.FCompSum / qocqot.FCompN
    qocqot.loc[:, 'FQoT'] = qocqot.FTeamSum / qocqot.FTeamN
    qocqot.loc[:, 'DQoC'] = qocqot.DCompSum / qocqot.DCompN
    qocqot.loc[:, 'DQoT'] = qocqot.DTeamSum / qocqot.DTeamN
    qocqot.loc[:, 'TOI60'] = qocqot.TOION / (qocqot.TOION + qocqot.TOIOFF)
    qocqot = qocqot.dropna().sort_values('TOI60', ascending=False)  # In case I have zeroes

    qocqot.loc[:, 'PlayerName'] = qocqot.PlayerID.apply(lambda x: helpers.get_lastname(players.player_as_str(x)))
    qocqot.loc[:, 'PlayerInitials'] = qocqot.PlayerID.apply(lambda x: helpers.get_lastname(players.player_as_str(x)))
    qocqot.loc[:, 'Position'] = qocqot.PlayerID.apply(lambda x: players.get_player_position(x))
    qocqot.drop({'FCompSum', 'FCompN', 'DCompSum', 'DCompN', 'FTeamSum', 'FTeamN', 'DTeamSum', 'DTeamN',
                 'PlayerID'}, axis=1, inplace=True)

    # Reorder columns for the parallel coordinates plot
    qocqot = qocqot[['FQoT', 'FQoC', 'DQoC', 'DQoT', 'TOION', 'TOIOFF', 'TOI60', 'PlayerName', 'PlayerInitials',
                     'Position']] \
        .sort_values('TOION', ascending=False) \
        .drop({'TOION', 'TOION', 'TOIOFF', 'TOI60'}, axis=1)

    fig, axes = plt.subplots(2, 2, sharex=True, sharey=True, figsize=[11, 7])

    forwards = qocqot.query('Position != "D"')
    centers = forwards.query('Position == "C"').drop('Position', axis=1).iloc[:6, :]
    wingers = forwards.query('Position != "C"').drop('Position', axis=1).iloc[:6, :]
    forwards.drop('Position', axis=1, inplace=True)
    vhelper.parallel_coords(forwards, centers, 'PlayerInitials', 'PlayerName', axes.flatten()[0])
    vhelper.parallel_coords(forwards, wingers, 'PlayerInitials', 'PlayerName', axes.flatten()[1])

    alldefense = qocqot.query('Position == "D"').drop('Position', axis=1)
    defense = alldefense.iloc[:6, :]
    vhelper.parallel_coords(alldefense, defense, 'PlayerInitials', 'PlayerName', axes.flatten()[2])

    other_players = pd.concat([qocqot.drop('Position', axis=1), centers, wingers, defense]) \
        .drop_duplicates(keep=False).iloc[:6, :]
    vhelper.parallel_coords(pd.concat([forwards, defense]), other_players, 'PlayerInitials', 'PlayerName', axes.flatten()[3])

    fig.text(0.5, 0.04, 'Statistic (based on TOI/60)', ha='center')
    fig.text(0.04, 0.5, 'Minutes', va='center', rotation='vertical')
    axes.flatten()[0].set_title('Top centers')
    axes.flatten()[1].set_title('Top wingers')
    axes.flatten()[2].set_title('Top defense')
    axes.flatten()[3].set_title('Others')

    fig.suptitle(_parallel_usage_chart_title(**kwargs))

    return vhelper.savefilehelper(**kwargs)


def animated_usage_chart(**kwargs):
    """

    :param kwargs:
    :return:
    """

    if 'roll_len_days' not in kwargs:
        kwargs['roll_len_days'] = 30

    qocqot = vhelper.get_and_filter_5v5_log(**kwargs)
    qocqot = qocqot[['PlayerID', 'TOION', 'TOIOFF', 'Game', 'Season',
                     'FCompSum', 'FCompN', 'DCompSum', 'DCompN',
                     'FTeamSum', 'FTeamN', 'DTeamSum', 'DTeamN']]
    qocqot.loc[:, 'FQoC'] = qocqot.FCompSum / qocqot.FCompN
    qocqot.loc[:, 'FQoT'] = qocqot.FTeamSum / qocqot.FTeamN
    qocqot.loc[:, 'DQoC'] = qocqot.DCompSum / qocqot.DCompN
    qocqot.loc[:, 'DQoT'] = qocqot.DTeamSum / qocqot.DTeamN
    qocqot.loc[:, 'TOI60'] = qocqot.TOION / (qocqot.TOION + qocqot.TOIOFF)

    qocqot = schedules.attach_game_dates_to_dateframe(qocqot).sort_values('Date')

    alldates = {i: date for i, date in enumerate(qocqot.Date.unique())}

    temp = qocqot.query('Date == "{0:s}"'.format(alldates[0]))
    scat = plt.scatter(temp.FQoC, temp.DQoC)

    def update(frame_number):
        temp = qocqot.query('Date == "{0:s}"'.format(alldates[frame_number]))
        data = temp[['FQoC', 'DQoC']].as_matrix()
        scat.set_offsets(data)
        plt.title('{0:d}-day rolling usage as of {1:s}'.format(kwargs['roll_len_days'], alldates[frame_number]))
        return scat,

    animation = FuncAnimation(plt.gcf(), update, blit=False, interval=1000)
    if 'save_file' in kwargs:
        animation.save(kwargs['save_file'])
    plt.show()


def _parallel_usage_chart_title(**kwargs):
    """

    :param kwargs:
    :return:
    """

    title = []
    title.append('Quality of competition and teammates')
    if 'team' in kwargs:
        title.append(team_info.team_as_str(kwargs['team']) + ', ')
    else:
        title.append('')
    title[-1] += '{0:s} to {1:s}'.format(*vhelper.get_startdate_enddate_from_kwargs(**kwargs))
    return '\n'.join(title)
