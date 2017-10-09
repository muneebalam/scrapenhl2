import scrapenhl2.scrape.scrape_setup as scrape_setup
import scrapenhl2.scrape.scrape_game as scrape_game
import os
import os.path
import feather
import pandas as pd
import json
import os.path
import urllib.request
import urllib.error
import datetime
import numpy as np
import logging
import halo

def get_pbp_events(*args, **kwargs):
    """
    A general method that yields a generator of dataframes of PBP events subject to given limitations.

    Keyword arguments are applied as "or" conditions for each individual keyword (e.g. multiple teams) but as
    "and" conditions otherwise.

    The non-keyword arguments are event types subject to "or" conditions:

    - 'fac' or 'faceoff'
    - 'shot' or 'sog' or 'save'
    - 'hit'
    - 'stop' or 'stoppage'
    - 'block' or 'blocked shot'
    - 'miss' or 'missed shot'
    - 'give' or 'giveaway'
    - 'take' or 'takeaway'
    - 'penl' or 'penalty'
    - 'goal'
    - 'period end'
    - 'period official'
    - 'period ready'
    - 'period start'
    - 'game scheduled'
    - 'gend' or 'game end'
    - 'shootout complete'
    - 'chal' or 'official challenge'
    - 'post', which is not an officially designated event but will be searched for

    Dataframes are returned season-by-season to save on memory. If you want to operate on all seasons,
    process this data before going to the next season.

    Defaults to return all regular-season and playoff events by all teams.

    Supported keyword arguments:

    - players_on_ice: str or int, or list of them, player IDs or names of players on ice for event.
    - players_on_ice_for: like players_on_ice, but players must be on ice for team that "did" event.
    - players_on_ice_ag: like players_on_ice, but players must be on ice for opponent of team that "did" event.
    - team, str or int, or list of them. Teams to filter for.
    - team_for, str or int, or list of them. Team that committed event.
    - team_ag, str or int, or list of them. Team that "received" event.
    - home_team: str or int, or list of them. Home team.
    - road_team: str or int, or list of them. Road team.
    - start_date: str or date, will only return data on or after this date. YYYY-MM-DD
    - end_date: str or date, will only return data on or before this date. YYYY-MM-DD
    - start_season: int, will only return events in or after this season. Defaults to 2010-11.
    - end_season: int, will only return events in or before this season. Defaults to current season.
    - season_type: int or list of int. 1 for preseason, 2 for regular, 3 for playoffs, 4 for ASG, 6 for Oly, 8 for WC
    - start_game: int, start game. Applies only to start season. Game ID will be this, or greater.
    - end_game: int, end game. Applies only to end season. Game ID will be this, or smaller.
    - acting_player: str or int, or list of them, players who committed event (e.g. took a shot).
    - receiving_player: str or int, or list of them, players who received event (e.g. took a hit).
    - strength_hr: tuples or list of them, e.g. (5, 5) or ((5, 5), (4, 4), (3, 3)). This is (Home, Road).
    - strength_to: tuples or list of them, e.g. (5, 5) or ((5, 5), (4, 4), (3, 3)). This is (Team, Opponent).
    - score_diff: int or list of them, acceptable score differences (e.g. 0 for tied, (1, 2, 3) for up by 1-3 goals)
    - start_time: int, seconds elapsed in game. Events returned will be after this.
    - end_time: int, seconds elapsed in game. Events returned will be before this.

    :param args: str, event types to search for (applied "OR", not "AND")
    :param kwargs: keyword arguments specifying filters (applied "AND", not "OR")
    :return: df, a pandas dataframe
    """

    # Read from team logs. Since I store by team, first, read relevant teams' logs
    all_teams_to_read = _teams_to_read(**kwargs)
    all_seasons_to_read = _seasons_to_read(**kwargs)

    for season in all_seasons_to_read:
        df = pd.concat([scrape_setup.get_team_pbp(season, team) for team in all_teams_to_read])
        df = _filter_for_team(df, **kwargs)

        df = _filter_for_games(df, **kwargs)

        df = _filter_for_times(df, **kwargs)

        df = _filter_for_strengths(df, **kwargs)

        df = _filter_for_event_types(df, *args)

        # This could take longest, since it involved reading TOI, so leave it until the end
        df = _filter_for_players(df, **kwargs)


def _filter_for_event_types(data, *args):
    """
    Uses
    :param data: a dataframe with pbp data
    :param args: args as given to get_pbp_events, for example
    :return: a dataframe filtered to fit event-related args
    """

    data.loc[:, 'Event2'] = data.Event.str.lower()

    dflst = []
    for arg in args:
        dflst.append(data[data.Event2 == scrape_setup.get_event_longname(arg)])
    data = pd.concat(dflst).drop('Event2', axis=1)
    return data


def _filter_for_scores(data, **kwargs):
    """
    Uses the score_diff keyword argument to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit score-related kwargs
    """

    if 'score_diff' in kwargs:
        if isinstance(kwargs['score_diff'], int):
            sds = set((kwargs['score_diff']))
        else:
            sds = set(kwargs['score_diff'])
        data = pd.concat([data[data.TeamScore - data.OppScore == sd] for sd in sds])
    return data


def _filter_for_strengths(data, **kwargs):
    """
    Uses the strength_hr and strength_to keyword arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit strength-related kwargs
    """

    if 'strength_to' in kwargs:
        data = data[(data.TeamStrength == kwargs['strength_to'][0]) & (data.OppStrength == kwargs['strength_to'][1])]

    if 'strength_hr' in kwargs:
        # Find whether team was home or road
        pass

    return data


def _filter_for_times(data, **kwargs):
    """
    Uses the start_time and end_time keyword arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit time-related kwargs
    """

    if 'start_time' in kwargs:
        data = data[data.Time >= kwargs['start_time']]
    if 'end_time' in kwargs:
        data = data[data.Time <= kwargs['end_time']]
    return data


def _filter_for_games(data, **kwargs):
    """
    Uses the start_game, end_game, and season_types keyword arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit game number-related kwargs
    """

    if 'start_game' in kwargs:
        startseason = data.Season.min()
        data = data[(data.Season == startseason) & (data.Game >= kwargs['start_game'])]
    if 'end_game' in kwargs:
        endseason = data.Season.max()
        data = data[(data.Season == endseason) & (data.Game <= kwargs['end_game'])]
    if 'season_type' in kwargs:
        if isinstance(kwargs['season_type'], int):
            stypes = set((kwargs['season_type']))
        else:
            stypes = set(kwargs['season_type'])
        data = pd.concat([data.Game // 10000 == stype for stype in stypes])
    return data


def _filter_for_players(data, **kwargs):
    """
    Uses the players_on_ice, players_on_ice_for, players_on_ice_ag, acting_player, and receiving_player keyword
    arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit player-related kwargs
    """

    if 'acting_player' in kwargs:
        p = scrape_setup.player_as_id(kwargs['acting_player'])
        data = data[data.Actor == p]

    if 'receiving_player' in kwargs:
        p = scrape_setup.player_as_id(kwargs['receiving_player'])
        data = data[data.Recipient == p]

    if 'players_on_ice' in kwargs or 'players_on_ice_for' in kwargs or 'players_on_ice_ag' in kwargs:
        # Now we know we need to read TOI
        dflst = []
        for season in set(data.Season):
            temp = data[data.Season == season]
            for game in set(temp.Game):
                dflst.append(_join_on_ice_players_to_pbp(season, game, temp[temp.Game == game]))
        data2 = pd.concat(dflst)

        if 'players_on_ice' in kwargs:
            players = set()
            key = 'players_on_ice'
            if key in kwargs:
                if scrape_setup.check_types(kwargs[key]):
                    players.add(kwargs[key])
                else:
                    players = players.union(kwargs[key])

            querystrings = []
            for hr in ('H', 'R'):
                for suf in ('1', '2', '3', '4', '5', '6', 'G'):
                    querystrings.append('{0:s}{1:s}'.format(hr, suf))
            querystring = ' & '.join(querystrings)
            data2 = data2.query(querystring)

        # TODO finish players_on_ice_for and _ag

    return data2


def _join_on_ice_players_to_pbp(season, game, pbp=None, toi=None):
    """

    :param season: int, the season
    :param game: int, the game
    :param pbp: df, the plays. If None, will read from file.
    :param toi: df, the shifts to join to plays. If None, will read from file.
    :return: df, pbp but augmented with on-ice players
    """

    if pbp is None:
        pbp = scrape_game.get_parsed_pbp(season, game)
    if toi is None:
        toi = scrape_game.get_parsed_toi(season, game)

    newpbp = pbp.merge(toi, how='left', on='Time')
    return newpbp


def _filter_for_team(data, **kwargs):
    """
    Uses the team, team_for, team_ag, home_team, and road_team keyword arguments to filter the data.
    :param data: a dataframe with pbp data
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a dataframe filtered to fit team-related kwargs
    """

    if 'team' in kwargs:
        teamid = scrape_setup.team_as_id(kwargs['team'])
        data = data[(data.Home == teamid) | (data.Road == teamid)]
    if 'team_for' in kwargs:
        teamid = scrape_setup.team_as_id(kwargs['team_for'])
        data = data[data.Team == teamid]
    if 'team_ag' in kwargs:
        teamid = scrape_setup.team_as_id(kwargs['team_ag'])
        data = data[((data.Home == teamid) | (data.Road == teamid)) & (data.Team != teamid)]

    if 'home_team' in kwargs:
        teamid = scrape_setup.team_as_id(kwargs['home_team'])
        data = data[data.Home == teamid]
    if 'road_team' in kwargs:
        teamid = scrape_setup.team_as_id(kwargs['road_team'])
        data = data[data.Road == teamid]

    return data


def _seasons_to_read(**kwargs):
    """
    Method uses start_date, end_date, start_season, and end_season to infer seasons to read
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: set of int (seasons)
    """

    minseason = 2011
    maxseason = scrape_setup.get_current_season()

    if 'start_season' in kwargs:
        minseason = max(kwargs['start_season'], minseason)
    if 'start_date' in kwargs:
        minseason = max(scrape_setup.infer_season_from_date(kwargs['start_date']), minseason)

    if 'end_season' in kwargs:
        maxseason = min(kwargs['end_season'], maxseason)
    if 'end_date' in kwargs:
        maxseason = max(scrape_setup.infer_season_from_date(kwargs['end_date']), maxseason)

    return list(range(minseason, maxseason + 1))


def _teams_to_read(**kwargs):
    """
    Method concatenates unique values from keyword arguments named team, team_for, and team_ag
    :param kwargs: kwargs as given to get_pbp_events, for example
    :return: a set of int (team IDs)
    """

    teamlst = set()
    for key in ('team', 'team_for', 'team_ag'):
        if key in kwargs:
            if isinstance(kwargs[key], str) or isinstance(kwargs[key], int):
                teamlst.add(scrape_setup.team_as_id(kwargs[key]))
            else:
                for val in kwargs[key]:
                    teamlst.add(scrape_setup.team_as_id(val))
    return teamlst