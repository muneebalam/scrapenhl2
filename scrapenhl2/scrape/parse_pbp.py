"""
This module contains methods for parsing PBP.
"""

import os.path

import numpy as np
import pandas as pd

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.manipulate_schedules as manipulate_schedules
import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.players as players
import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.scrape_pbp as scrape_pbp


def parse_season_pbp(season, force_overwrite=False):
    """
    Parses pbp from the given season.

    :param season: int, the season
    :param force_overwrite: bool. If true, parses all games. If false, only previously unparsed ones

    :return: nothing
    """
    if season is None:
        season = schedules.get_current_season()

    sch = schedules.get_season_schedule(season)
    games = sch[sch.Status == "Final"].Game.values
    games.sort()
    intervals = helpers.intervals(games)
    interval_j = 0
    for i, game in enumerate(games):
        try:
            parse_game_pbp(season, game, force_overwrite)
        except Exception as e:
            pass  # ed.print_and_log('{0:d} {1:d} {2:s}'.format(season, game, str(e)), 'warn')
        if interval_j < len(intervals):
            if i == intervals[interval_j][0]:
                print('Done parsing through {0:d} {1:d} ({2:d}%)'.format(
                    season, game, round(intervals[interval_j][0] / len(games) * 100)))
                interval_j += 1


def get_parsed_pbp(season, game):
    """
    Loads the compressed json file containing this game's play by play from disk.

    :param season: int, the season
    :param game: int, the game

    :return: json, the json pbp
    """
    return pd.read_hdf(get_game_parsed_pbp_filename(season, game))


def save_parsed_pbp(pbp, season, game):
    """
    Saves the pandas dataframe containing pbp information to disk as an HDF5.

    :param pbp: df, a pandas dataframe with the pbp of the game
    :param season: int, the season
    :param game: int, the game

    :return: nothing
    """
    pbp.to_hdf(get_game_parsed_pbp_filename(season, game),
               key='P{0:d}0{1:d}'.format(season, game),
               mode='w', complib='zlib')


def _create_pbp_df_json(pbp, gameinfo):
    """
    Creates a pandas dataframe from the pbp, making use of gameinfo (from schedule file) as well

    :param pbp: dict, from pbp json
    :param gameinfo: dict, single row from schedule file

    :return: dataframe
    """

    index = [i for i in range(len(pbp))]
    period = ['' for _ in range(len(pbp))]
    times = ['0:00' for _ in range(len(pbp))]
    event = ['NA' for _ in range(len(pbp))]

    team = [-1 for _ in range(len(pbp))]
    p1 = [-1 for _ in range(len(pbp))]
    p1role = ['' for _ in range(len(pbp))]
    p2 = [-1 for _ in range(len(pbp))]
    p2role = ['' for _ in range(len(pbp))]
    xs = [np.NaN for _ in range(len(pbp))]
    ys = [np.NaN for _ in range(len(pbp))]
    note = ['' for _ in range(len(pbp))]

    for i in range(len(pbp)):
        period[i] = helpers.try_to_access_dict(pbp, i, 'about', 'period', default_return='')
        times[i] = helpers.try_to_access_dict(pbp, i, 'about', 'periodTime', default_return='0:00')
        event[i] = helpers.try_to_access_dict(pbp, i, 'result', 'event', default_return='NA')

        xs[i] = float(helpers.try_to_access_dict(pbp, i, 'coordinates', 'x', default_return=np.NaN))
        ys[i] = float(helpers.try_to_access_dict(pbp, i, 'coordinates', 'y', default_return=np.NaN))
        team[i] = helpers.try_to_access_dict(pbp, i, 'team', 'id', default_return=-1)

        p1[i] = helpers.try_to_access_dict(pbp, i, 'players', 0, 'player', 'id', default_return=-1)
        p1role[i] = helpers.try_to_access_dict(pbp, i, 'players', 0, 'playerType', default_return='')
        p2[i] = helpers.try_to_access_dict(pbp, i, 'players', 1, 'player', 'id', default_return=-1)
        p2role[i] = helpers.try_to_access_dict(pbp, i, 'players', 1, 'playerType', default_return='')

        note[i] = helpers.try_to_access_dict(pbp, i, 'result', 'description', default_return='')

    # Switch blocked shots from being an event for player who blocked, to player who took shot that was blocked
    # That means switching team attribution and actor/recipient.
    # TODO: why does schedule have str, not int, home and road here?
    switch_teams = {gameinfo['Home']: gameinfo['Road'], gameinfo['Road']: gameinfo['Home']}
    team_sw = [team[i] if event[i] != "Blocked Shot" else switch_teams[team[i]] for i in range(len(team))]
    p1_sw = [p1[i] if event[i] != "Blocked Shot" else p2[i] for i in range(len(p1))]
    p2_sw = [p2[i] if event[i] != "Blocked Shot" else p1[i] for i in range(len(p2))]
    p1role_sw = [p1role[i] if event[i] != "Blocked Shot" else p2role[i] for i in range(len(p1role))]
    p2role_sw = [p2role[i] if event[i] != "Blocked Shot" else p1role[i] for i in range(len(p2role))]

    pbpdf = pd.DataFrame({'Index': index, 'Period': period, 'MinSec': times, 'Event': event,
                          'Team': team_sw, 'Actor': p1_sw, 'ActorRole': p1role_sw, 'Recipient': p2_sw,
                          'RecipientRole': p2role_sw, 'X': xs, 'Y': ys, 'Note': note})
    return pbpdf


def _add_scores_to_pbp(pbpdf, gameinfo):
    """
    Adds columns for home and road goals to supplied dataframe

    :param pbp: dataframe of play by play events
    :param gameinfo: dict, one row of the schedule file

    :return: dataframe with two extra columns
    """
    # Add score
    homegoals = pbpdf[['Event', 'Period', 'MinSec', 'Team']] \
        .query('Team == {0:d} & Event == "Goal"'.format(gameinfo['Home']))
    # TODO check team log for value_counts() of Event.
    roadgoals = pbpdf[['Event', 'Period', 'MinSec', 'Team']] \
        .query('Team == {0:d} & Event == "Goal"'.format(gameinfo['Road']))

    if len(homegoals) > 0:  # errors if len is 0
        homegoals.loc[:, 'HomeScore'] = 1
        homegoals.loc[:, 'HomeScore'] = homegoals.HomeScore.cumsum()
        pbpdf = pbpdf.merge(homegoals, how='left', on=['Event', 'Period', 'MinSec', 'Team'])

    if len(roadgoals) > 0:
        roadgoals.loc[:, 'RoadScore'] = 1
        roadgoals.loc[:, 'RoadScore'] = roadgoals.RoadScore.cumsum()
        pbpdf = pbpdf.merge(roadgoals, how='left', on=['Event', 'Period', 'MinSec', 'Team'])
        # TODO check: am I counting shootout goals?

    # Make the first row show 0 for both teams
    # TODO does this work for that one game that got stopped?
    # Maybe I should fill forward first, then replace remaining NA with 0
    pbpdf.loc[pbpdf.Index == 0, 'HomeScore'] = 0
    pbpdf.loc[pbpdf.Index == 0, 'RoadScore'] = 0

    # And now forward fill
    pbpdf.loc[:, "HomeScore"] = pbpdf.HomeScore.fillna(method='ffill')
    pbpdf.loc[:, "RoadScore"] = pbpdf.RoadScore.fillna(method='ffill')
    return pbpdf


def _add_times_to_pbp(pbpdf):
    """
    Uses period and time columns to add a column with time in seconds elapsed in game

    :param pbp: df, pandas dataframe

    :return: pandas dataframe
    """

    # Convert MM:SS and period to time in game
    minsec = pbpdf.MinSec.str.split(':', expand=True)
    minsec.columns = ['Min', 'Sec']
    minsec.Period = pbpdf.Period
    minsec.loc[:, 'Min'] = pd.to_numeric(minsec.loc[:, 'Min'])
    minsec.loc[:, 'Sec'] = pd.to_numeric(minsec.loc[:, 'Sec'])
    minsec.loc[:, 'TimeInPeriod'] = 60 * minsec.Min + minsec.Sec

    minsec.loc[:, 'PeriodContribution'] = minsec.Period.apply(helpers.period_contribution)
    minsec.loc[:, 'Time'] = minsec.PeriodContribution + minsec.TimeInPeriod
    pbpdf.loc[:, 'Time'] = minsec.Time
    return pbpdf


def read_events_from_page(rawpbp, season, game):
    """
    This method takes the json pbp and returns a pandas dataframe with the following columns:

    * Index: int, index of event
    * Period: str, period of event. In regular season, could be 1, 2, 3, OT, or SO. In playoffs, 1, 2, 3, 4, 5...
    * MinSec: str, m:ss, time elapsed in period
    * Time: int, time elapsed in game
    * Event: str, the event name
    * Team: int, the team id. Note that this is switched to blocked team for blocked shots to ease Corsi calculations.
    * Actor: int, the acting player id. Switched with recipient for blocks (see above)
    * ActorRole: str, e.g. for faceoffs there is a "Winner" and "Loser". Switched with recipient for blocks (see above)
    * Recipient: int, the receiving player id. Switched with actor for blocks (see above)
    * RecipientRole: str, e.g. for faceoffs there is a "Winner" and "Loser". Switched with actor for blocks (see above)
    * X: int, the x coordinate of event (or NaN)
    * Y: int, the y coordinate of event (or NaN)
    * Note: str, additional notes, which may include penalty duration, assists on a goal, etc.

    :param rawpbp: json, the raw json pbp
    :param season: int, the season
    :param game: int, the game

    :return: pandas dataframe, the pbp in a nicer format
    """
    pbp = helpers.try_to_access_dict(rawpbp, 'liveData', 'plays', 'allPlays')
    if pbp is None:
        return

    gameinfo = schedules.get_game_data_from_schedule(season, game)
    pbpdf = _create_pbp_df_json(pbp, gameinfo)
    if len(pbpdf) == 0:
        return pbpdf

    pbpdf = _add_scores_to_pbp(pbpdf, gameinfo)
    pbpdf = _add_times_to_pbp(pbpdf)

    return pbpdf


def get_game_parsed_pbp_filename(season, game):
    """
    Returns the filename of the parsed pbp folder

    :param season: int, current season
    :param game: int, game

    :return: str, /scrape/data/parsed/pbp/[season]/[game].zlib
    """
    return os.path.join(organization.get_season_parsed_pbp_folder(season), str(game) + '.h5')


def parse_game_pbp(season, game, force_overwrite=False):
    """
    Reads the raw pbp from file, updates player IDs, updates player logs, and parses the JSON to a pandas DF
    and writes to file. Also updates team logs accordingly.

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If True, will execute. If False, executes only if file does not exist yet.

    :return: True if parsed, False if not
    """

    filename = get_game_parsed_pbp_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    # Looks like 2010-11 is the first year where this feed supplies more than just boxscore data
    rawpbp = scrape_pbp.get_raw_pbp(season, game)
    players.update_player_ids_from_page(rawpbp)
    players.update_player_logs_from_page(rawpbp, season, game)
    manipulate_schedules.update_schedule_with_coaches(rawpbp, season, game)
    manipulate_schedules.update_schedule_with_result_using_pbp(rawpbp, season, game)

    parsedpbp = read_events_from_page(rawpbp, season, game)
    save_parsed_pbp(parsedpbp, season, game)
    # ed.print_and_log('Parsed events for {0:d} {1:d}'.format(season, game), print_and_log=False)
    return True


def parse_game_pbp_from_html(season, game, force_overwrite=False):
    """
    Reads the raw pbp from file, updates player IDs, updates player logs, and parses the JSON to a pandas DF
    and writes to file. Also updates team logs accordingly.

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If True, will execute. If False, executes only if file does not exist yet.

    :return: True if parsed, False if not
    """

    filename = scrape_pbp.get_game_pbplog_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    rawpbp = scrape_pbp.save(season, game)
    players.update_player_ids_from_page(rawpbp)
    manipulate_schedules.update_player_logs_from_page(rawpbp, season, game)
    manipulate_schedules.update_schedule_with_coaches(rawpbp, season, game)
    manipulate_schedules.update_schedule_with_result(rawpbp, season, game)

    parsedpbp = read_events_from_page(rawpbp, season, game)
    save_parsed_pbp(parsedpbp, season, game)
    # ed.print_and_log('Parsed events for {0:d} {1:d}'.format(season, game), print_and_log=False)
    return True


def parse_pbp_setup():
    """
    Creates parsed pbp folders if need be

    :return: nothing
    """
    for season in range(2005, schedules.get_current_season() + 1):
        organization.check_create_folder(organization.get_season_parsed_pbp_folder(season))


parse_pbp_setup()
