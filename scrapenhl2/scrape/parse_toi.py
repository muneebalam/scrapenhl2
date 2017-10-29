"""
This module contains methods for parsing TOI.
"""

import os.path
import re

import pandas as pd

import scrapenhl2.scrape.general_helpers as helpers
import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.players as players
import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.scrape_toi as scrape_toi


def parse_season_toi(season, force_overwrite=False):
    """
    Parses toi from the given season. Final games covered only.

    :param season: int, the season
    :param force_overwrite: bool. If true, parses all games. If false, only previously unparsed ones

    :return:
    """

    if season is None:
        season = schedules.get_current_season()

    sch = schedules.get_season_schedule(season)
    games = sch[sch.Status == "Final"].Game.values
    games.sort()
    for game in games:
        parse_game_toi(season, game, force_overwrite)


def parse_game_toi(season, game, force_overwrite=False):
    """
    Parses TOI from json for this game

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If True, will execute. If False, executes only if file does not exist yet.

    :return: nothing
    """
    filename = get_game_parsed_toi_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    # TODO for some earlier seasons I need to read HTML instead. Also for live games
    # Looks like 2010-11 is the first year where this feed supplies more than just boxscore data
    rawtoi = scrape_toi.get_raw_toi(season, game)
    try:
        parsedtoi = read_shifts_from_page(rawtoi, season, game)
    except ValueError as ve:
        # ed.print_and_log('Error with {0:d} {1:d}'.format(season, game), 'warning')
        # ed.print_and_log(str(ve), 'warning')  # TODO look through 2016, getting some errors
        parsedtoi = None

    if parsedtoi is None:
        return False

    # PbP doesn't have strengths, so let's add those in
    # Ok maybe leave strengths, scores, etc, for team logs
    # update_pbp_from_toi(parsedtoi, season, game)
    save_parsed_toi(parsedtoi, season, game)
    # ed.print_and_log('Parsed shifts for {0:d} {1:d}'.format(season, game))
    return True


def parse_game_toi_from_html(season, game, force_overwrite=False):
    """
    Parses TOI from the html shift log from this game.

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If True, will execute. If False, executes only if file does not exist yet.

    :return: nothing
    """
    # TODO force_overwrite support
    filenames = (scrape_toi.get_home_shiftlog_filename(season, game),
                 scrape_toi.get_road_shiftlog_filename(season, game))
    if force_overwrite is False and os.path.exists(scrape_toi.get_home_shiftlog_filename(season, game)) and \
            os.path.exists(scrape_toi.get_home_shiftlog_filename(season, game)):
        return False

    gameinfo = schedules.get_game_data_from_schedule(season, game)
    try:
        parsedtoi = read_shifts_from_html_pages(scrape_toi.get_raw_html_toi(season, game, 'H'),
                                                scrape_toi.get_raw_html_toi(season, game, 'R'),
                                                gameinfo['Home'], gameinfo['Road'],
                                                season, game)
    except ValueError as ve:
        # ed.print_and_log('Error with {0:d} {1:d}'.format(season, game), 'warning')
        # ed.print_and_log(str(ve), 'warning')
        parsedtoi = None

    save_parsed_toi(parsedtoi, season, game)
    # ed.print_and_log('Parsed shifts for {0:d} {1:d}'.format(season, game))
    return True


def get_parsed_toi(season, game):
    """
    Loads the compressed json file containing this game's shifts from disk.

    :param season: int, the season
    :param game: int, the game

    :return: json, the json shifts
    """
    return pd.read_hdf(get_game_parsed_toi_filename(season, game))


def save_parsed_toi(toi, season, game):
    """
    Saves the pandas dataframe containing shift information to disk as an HDF5.

    :param toi: df, a pandas dataframe with the shifts of the game
    :param season: int, the season
    :param game: int, the game

    :return: nothing
    """
    toi.to_hdf(get_game_parsed_toi_filename(season, game),
               key='T{0:d}0{1:d}'.format(season, game),
               mode='w', complib='zlib')


def read_shifts_from_html_pages(rawtoi1, rawtoi2, teamid1, teamid2, season, game):
    """
    Aggregates information from two html pages given into a dataframe with one row per second and one col per player.

    :param rawtoi1: str, html page of shift log for team id1
    :param rawtoi2: str, html page of shift log for teamid2
    :param teamid1: int, team id corresponding to rawtoi1
    :param teamid2: int, team id corresponding to rawtoi1
    :param season: int, the season
    :param game: int, the game

    :return: dataframe
    """

    from html_table_extractor.extractor import Extractor
    dflst = []
    for rawtoi, teamid in zip((rawtoi1, rawtoi2), (teamid1, teamid2)):
        extractor = Extractor(rawtoi)
        extractor.parse()
        tables = extractor.return_list()

        ids = []
        periods = []
        starts = []
        ends = []
        durationtime = []
        teams = []
        i = 0
        while i < len(tables):
            # A convenient artefact of this package: search for [p, p, p, p, p, p, p, p]
            if len(tables[i]) == 8 and helpers.check_number_last_first_format(tables[i][0]):
                pname = helpers.remove_leading_number(tables[i][0])
                pname = helpers.flip_first_last(pname)
                pid = players.player_as_id(pname)
                i += 2  # skip the header row
                while re.match('\d{1,2}', tables[i][0]):  # First entry is shift number
                    # print(tables[i])
                    shiftnum, per, start, end, dur, ev = tables[i]
                    # print(pname, pid, shiftnum, per, start, end)
                    ids.append(pid)
                    periods.append(int(per))
                    starts.append(start[:start.index('/')].strip())
                    ends.append(end[:end.index('/')].strip())
                    durationtime.append(helpers.mmss_to_secs(dur))
                    teams.append(teamid)
                    i += 1
                i += 1
            else:
                i += 1

        startmin = [x[:x.index(':')] for x in starts]
        startsec = [x[x.index(':') + 1:] for x in starts]
        starttimes = [1200 * (p - 1) + 60 * int(m) + int(s) + 1 for p, m, s in zip(periods, startmin, startsec)]
        # starttimes = [0 if x == 1 else x for x in starttimes]
        endmin = [x[:x.index(':')] for x in ends]
        endsec = [x[x.index(':') + 1:] for x in ends]
        # There is an extra -1 in endtimes to avoid overlapping start/end
        endtimes = [1200 * (p - 1) + 60 * int(m) + int(s) for p, m, s in zip(periods, endmin, endsec)]

        durationtime = [e - s for s, e in zip(starttimes, endtimes)]

        df = pd.DataFrame({'PlayerID': ids, 'Period': periods, 'Start': starttimes, 'End': endtimes,
                           'Team': teams, 'Duration': durationtime})
        dflst.append(df)

    return _finish_toidf_manipulations(pd.concat(dflst), season, game)


def read_shifts_from_page(rawtoi, season, game):
    """
    Turns JSON shift start-ends into TOI matrix with one row per second and one col per player

    :param rawtoi: dict, json from NHL API
    :param season: int, the season
    :param game: int, the game

    :return: dataframe
    """
    toi = rawtoi['data']
    if len(toi) == 0:
        return
    ids = ['' for _ in range(len(toi))]
    periods = [0 for _ in range(len(toi))]
    starts = ['0:00' for _ in range(len(toi))]
    ends = ['0:00' for _ in range(len(toi))]
    teams = ['' for _ in range(len(toi))]
    durations = [0 for _ in range(len(toi))]

    # The shifts are ordered shortest duration to longest.
    for i, dct in enumerate(toi):
        ids[i] = helpers.try_to_access_dict(dct, 'playerId', default_return='')
        periods[i] = helpers.try_to_access_dict(dct, 'period', default_return=0)
        starts[i] = helpers.try_to_access_dict(dct, 'startTime', default_return='0:00')
        ends[i] = helpers.try_to_access_dict(dct, 'endTime', default_return='0:00')
        durations[i] = helpers.try_to_access_dict(dct, 'duration', default_return=0)
        teams[i] = helpers.try_to_access_dict(dct, 'teamId', default_return='')

    gameinfo = schedules.get_game_data_from_schedule(season, game)

    # I originally took start times at face value and subtract 1 from end times
    # This caused problems with joining events--when there's a shot and the goalie freezes immediately
    # then, when you join this to the pbp, you'll get the players on the ice for the next draw as having
    # been on ice for the shot.
    # So I switch to adding 1 to start times, and leaving end times as-are.
    # That means that when joining on faceoffs, add 1 to faceoff times.
    # Exception: start time 1 --> start time 0
    startmin = [x[:x.index(':')] for x in starts]
    startsec = [x[x.index(':') + 1:] for x in starts]
    starttimes = [1200 * (p - 1) + 60 * int(m) + int(s) + 1 for p, m, s in zip(periods, startmin, startsec)]
    # starttimes = [0 if x == 1 else x for x in starttimes]
    endmin = [x[:x.index(':')] for x in ends]
    endsec = [x[x.index(':') + 1:] for x in ends]
    # There is an extra -1 in endtimes to avoid overlapping start/end
    endtimes = [1200 * (p - 1) + 60 * int(m) + int(s) for p, m, s in zip(periods, endmin, endsec)]

    durationtime = [e - s for s, e in zip(starttimes, endtimes)]

    df = pd.DataFrame({'PlayerID': ids, 'Period': periods, 'Start': starttimes, 'End': endtimes,
                       'Team': teams, 'Duration': durationtime})

    return _finish_toidf_manipulations(df, season, game)


def _finish_toidf_manipulations(df, season, game):
    """
    Takes dataframe of shifts (one row per shift) and makes into a matrix of players on ice for each second.

    :param df: dataframe
    :param season: int, the season
    :param game: int, the game

    :return: dataframe
    """
    gameinfo = schedules.get_game_data_from_schedule(season, game)

    # TODO don't read end times. Use duration, which has good coverage, to infer end. Then end + 1200 not needed below.
    # Sometimes shifts have the same start and time.
    # By the time we're here, they'll have start = end + 1
    # So let's remove shifts with duration -1
    df = df[df.Start != df.End + 1]

    # Sometimes you see goalies with a shift starting in one period and ending in another
    # This is to help in those cases.
    if sum(df.End < df.Start) > 0:
        # ed.print_and_log('Have to adjust a shift time', 'warn')
        # TODO I think I'm making a mistake with overtime shifts--end at 3900!
        # TODO also, maybe only go to the end of the period, not to 1200
        # ed.print_and_log(df[df.End < df.Start])
        df.loc[df.End < df.Start, 'End'] = df.loc[df.End < df.Start, 'End'] + 1200
    # One issue coming up is when the above line comes into play--missing times are filled in as 0:00
    tempdf = df[['PlayerID', 'Start', 'End', 'Team', 'Duration']].query("Duration > 0")
    tempdf = tempdf.assign(Time=tempdf.Start)
    # print(tempdf.head(20))

    # Let's filter out goalies for now. We can add them back in later.
    # This will make it easier to get the strength later
    pids = players.get_player_ids_file()
    tempdf = tempdf.merge(pids[['ID', 'Pos']], how='left', left_on='PlayerID', right_on='ID')

    # toi = pd.DataFrame({'Time': [i for i in range(0, max(df.End) + 1)]})
    toi = pd.DataFrame({'Time': [i for i in range(0, max(df.End))]})

    # Originally used a hacky way to fill in times between shift start and end: increment tempdf by one, filter, join
    # Faster to work with base structures
    # Or what if I join each player to full df, fill backward on start and end, and filter out rows where end > time
    # toidict = toi.to_dict(orient='list')
    # players_by_sec = [[] for _ in range(min(toidict['Start'], toidict['End'] + 1))]
    # for i in range(len(players_by_sec)):
    #    for j in range(toidict['Start'][i], toidict['End'][i] + 1):
    #        players_by_sec[j].append(toidict['PlayerID'][i])
    # Maybe I can create a matrix with rows = time and columns = players
    # Loop over start and end, and use iloc[] to set booleans en masse.
    # Then melt and filter

    # Create one row per second
    alltimes = toi.Time
    newdf = pd.DataFrame(index=alltimes)

    # Add rows and set times to True simultaneously
    for i, (pid, start, end, team, duration, time, pid, pos) in tempdf.iterrows():
        newdf.loc[start:end, pid] = True

    # Fill NAs to False
    for col in newdf:
        newdf.loc[:, col] = newdf[col].fillna(False)

    # Go wide to long and then drop unneeded rows
    newdf = newdf.reset_index().melt(id_vars='Time', value_vars=newdf.columns,
                                     var_name='PlayerID', value_name='OnIce')
    newdf = newdf[newdf.OnIce].drop('OnIce', axis=1)
    newdf = newdf.merge(tempdf.drop('Time', axis=1), how='left', on='PlayerID') \
        .query("Time <= End & Time >= Start") \
        .drop('ID', axis=1)

    # In case there were rows that were all missing, join onto TOI
    tempdf = toi.merge(newdf, how='left', on='Time')
    # TODO continue here--does newdf match tempdf after sort_values?

    # Old method
    # toidfs = []
    # while len(tempdf.index) > 0:
    #    temptoi = toi.merge(tempdf, how='inner', on='Time')
    #    toidfs.append(temptoi)

    #    tempdf = tempdf.assign(Time=tempdf.Time + 1)
    #    tempdf = tempdf.query('Time <= End')

    # tempdf = pd.concat(toidfs)
    # tempdf = tempdf.sort_values(by='Time')

    goalies = tempdf[tempdf.Pos == 'G'].drop({'Pos'}, axis=1)
    tempdf = tempdf[tempdf.Pos != 'G'].drop({'Pos'}, axis=1)

    # Append team name to start of columns by team
    home = str(gameinfo['Home'])
    road = str(gameinfo['Road'])

    # Goalies
    # Let's assume we get only one goalie per second per team.
    # TODO: flag if there are multiple listed and pick only one
    goalies.loc[:, 'GTeam'] = goalies.Team.apply(lambda x: 'HG' if str(int(x)) == home else 'RG')
    try:
        goalies2 = goalies[['Time', 'PlayerID', 'GTeam']] \
            .pivot(index='Time', columns='GTeam', values='PlayerID') \
            .reset_index()
    except ValueError:
        # Duplicate entries in index error.
        # ed.print_and_log('Multiple goalies for a team in {0:d} {1:d}, picking one with the most TOI'.format(
        #    season, game), 'warn')

        # Find times with multiple goalies
        too_many_goalies_h = goalies[goalies.GTeam == 'HG'][['Time']] \
            .assign(GoalieCount=1) \
            .groupby('Time').count() \
            .reset_index() \
            .query('GoalieCount > 1')

        too_many_goalies_r = goalies[goalies.GTeam == 'RG'][['Time']] \
            .assign(GoalieCount=1) \
            .groupby('Time').count() \
            .reset_index() \
            .query('GoalieCount > 1')

        # Find most common goalie for each team
        if len(too_many_goalies_h) == 0:
            problem_times_revised_h = goalies
        else:  # i.e. if len(too_many_goalies_h) > 0:
            top_goalie_h = goalies[goalies.GTeam == 'HG'][['PlayerID']] \
                .assign(GoalieCount=1) \
                .groupby('PlayerID').count() \
                .reset_index() \
                .sort_values('GoalieCount', ascending=False) \
                .PlayerID.iloc[0]
            # and now finally drop problem times
            problem_times_revised_h = goalies \
                .merge(too_many_goalies_h[['Time']], how='outer', on='Time', indicator=True)
            problem_times_revised_h.loc[:, 'ToDrop'] = (problem_times_revised_h._merge == 'both') & \
                                                       (problem_times_revised_h.PlayerID != top_goalie_h)
            problem_times_revised_h = problem_times_revised_h[problem_times_revised_h.ToDrop == False] \
                .drop({'_merge', 'ToDrop'}, axis=1)

        if len(too_many_goalies_r) == 0:
            problem_times_revised_r = problem_times_revised_h
        else:  # i.e. if len(too_many_goalies_r) > 0:
            top_goalie_r = goalies[goalies.GTeam == 'RG'][['PlayerID']] \
                .assign(GoalieCount=1) \
                .groupby('PlayerID').count() \
                .reset_index() \
                .sort_values('GoalieCount', ascending=False) \
                .PlayerID.iloc[0]
            problem_times_revised_r = problem_times_revised_h \
                .merge(too_many_goalies_r[['Time']], how='outer', on='Time', indicator=True)
            problem_times_revised_r.loc[:, 'ToDrop'] = (problem_times_revised_r._merge == 'both') & \
                                                       (problem_times_revised_r.PlayerID != top_goalie_r)
            problem_times_revised_r = problem_times_revised_r[problem_times_revised_r.ToDrop == False] \
                .drop({'_merge', 'ToDrop'}, axis=1)

        # Pivot again
        goalies2 = problem_times_revised_r[['Time', 'PlayerID', 'GTeam']] \
            .pivot(index='Time', columns='GTeam', values='PlayerID') \
            .reset_index()

    # Home
    hdf = tempdf.query('Team == "' + home + '"').sort_values(['Time', 'Duration'], ascending=[True, False])
    hdf2 = hdf[['Time', 'Duration']].groupby('Time').rank(method='first', ascending=False)
    hdf2 = hdf2.rename(columns={'Duration': 'rank'})
    hdf2.loc[:, 'rank'] = hdf2['rank'].apply(lambda x: int(x))
    hdf.loc[:, 'rank'] = 'H' + hdf2['rank'].astype('str')

    rdf = tempdf.query('Team == "' + road + '"').sort_values(['Time', 'Duration'], ascending=[True, False])
    rdf2 = rdf[['Time', 'Duration']].groupby('Time').rank(method='first', ascending=False)
    rdf2 = rdf2.rename(columns={'Duration': 'rank'})
    rdf2.loc[:, 'rank'] = rdf2['rank'].apply(lambda x: int(x))
    rdf.loc[:, 'rank'] = 'R' + rdf2['rank'].astype('str')

    # Remove values above 6--looking like there won't be many
    # But in those cases take shifts with longest durations
    # That's why we create hdf and rdf by also sorting by Time and Duration above, and select duration for rank()
    if len(hdf[hdf['rank'] == "H7"]) > 0:
        # ed.print_and_log('Some times from {0:d} {1:d} have too many home players; cutting off at 6'.format(
        #    season, game), 'warn')
        # ed.print_and_log('Longest shift being lost was {0:d} seconds'.format(
        #    hdf[hdf['rank'] == "H7"].Duration.max()), 'warn')
        pass
    if len(rdf[rdf['rank'] == "R7"]) > 0:
        # ed.print_and_log('Some times from {0:d} {1:d} have too many road players; cutting off at 6'.format(
        #    season, game), 'warn')
        # ed.print_and_log('Longest shift being lost was {0:d} seconds'.format(
        #    rdf[rdf['rank'] == "H7"].Duration.max()), 'warn')
        pass

    hdf = hdf.pivot(index='Time', columns='rank', values='PlayerID').iloc[:, 0:6]
    hdf.reset_index(inplace=True)  # get time back as a column
    rdf = rdf.pivot(index='Time', columns='rank', values='PlayerID').iloc[:, 0:6]
    rdf.reset_index(inplace=True)

    toi = toi.merge(hdf, how='left', on='Time') \
        .merge(rdf, how='left', on='Time') \
        .merge(goalies2, how='left', on='Time')

    column_order = list(toi.columns.values)
    column_order = ['Time'] + [x for x in sorted(column_order[1:])]  # First entry is Time; sort rest
    toi = toi[column_order]
    # Now should be Time, H1, H2, ... HG, R1, R2, ..., RG

    # For games in the first, HG and RG may not exist yet. Have dummy replacements in there.
    # Will be wrong for when goalie is pulled in first, but oh well...
    if 'HG' not in toi.columns:
        newcol = [0 for _ in range(len(toi))]
        toi.insert(loc=toi.columns.get_loc('R1'), column='HG', value=newcol)
    if 'RG' not in toi.columns:
        toi.loc[:, 'RG'] = 0

    toi.loc[:, 'HomeSkaters'] = 0
    for col in toi.loc[:, 'H1':'HG'].columns[:-1]:
        toi.loc[:, 'HomeSkaters'] = toi[col].notnull() + toi.HomeSkaters
    toi.loc[:, 'HomeSkaters'] = 100 * toi['HG'].notnull() + toi.HomeSkaters  # a hack to make it easy to recognize
    toi.loc[:, 'RoadSkaters'] = 0
    for col in toi.loc[:, 'R1':'RG'].columns[:-1]:
        toi.loc[:, 'RoadSkaters'] = toi[col].notnull() + toi.RoadSkaters
    toi.loc[:, 'RoadSkaters'] = 100 * toi['RG'].notnull() + toi.RoadSkaters  # a hack to make it easy to recognize

    # This is how we label strengths: 5 means 5 skaters plus goalie; five skaters w/o goalie is 4+1.
    toi.loc[:, 'HomeStrength'] = toi.HomeSkaters.apply(
        lambda x: '{0:d}'.format(x - 100) if x >= 100 else '{0:d}+1'.format(x - 1))
    toi.loc[:, 'RoadStrength'] = toi.RoadSkaters.apply(
        lambda x: '{0:d}'.format(x - 100) if x >= 100 else '{0:d}+1'.format(x - 1))

    toi.drop({'HomeSkaters', 'RoadSkaters'}, axis=1, inplace=True)

    # Also drop -1+1 and 0+1 cases, which are clearly errors, and the like.
    # Need at least 3 skaters apiece, 1 goalie apiece, time, and strengths to be non-NA = 11 non NA values
    toi2 = toi.dropna(axis=0, thresh=11)  # drop rows without at least 11 non-NA values
    if len(toi2) < len(toi):
        # ed.print_and_log('Dropped {0:d}/{1:d} times in {2:d} {3:d} because of invalid strengths'.format(
        #    len(toi) - len(toi2), len(toi), season, game), 'warn')
        pass

    # TODO data quality check that I don't miss times in the middle of the game

    return toi2


def get_game_parsed_toi_filename(season, game):
    """
    Returns the filename of the parsed toi folder

    :param season: int, current season
    :param game: int, game

    :return: str, /scrape/data/parsed/toi/[season]/[game].zlib
    """
    return os.path.join(organization.get_season_parsed_toi_folder(season), str(game) + '.h5')


def parse_toi_setup():
    """
    Creates parsed toi folders if need be

    :return:
    """
    for season in range(2005, schedules.get_current_season() + 1):
        organization.check_create_folder(organization.get_season_parsed_toi_folder(season))


parse_toi_setup()
