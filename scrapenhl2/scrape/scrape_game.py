import scrapenhl2.scrape.scrape_setup as ss  # lots of helpful methods in this module
import scrapenhl2.scrape.exception_decor as ed
import pandas as pd  # standard scientific python stack
import numpy as np  # standard scientific python stack
import os  # for files
import os.path  # for files
import json  # NHL API outputs JSONs
import urllib.request  # for accessing internet pages
import urllib.error  # for errors in accessing internet pages
import zlib  # for compressing and saving files
from time import sleep  # this frees up time for use as variable name
import pyarrow  # related to feather; need to import to use an error
import logging  # for error logging
import halo  # terminal spinners

# Known issues (non-exhaustive list)
# 2016 20099, home team does not have players listed on ice for final 25 seconds of regulation
# 2016 20163, road team does not have players listed last 8 seconds
# Similar in 2016: 20408, 20419, 20421, 20475, 20510, 20511, 21163, 21194, 30185
# 2016 20598 road team has too many players, losing a 9 second shift; 30144 loses an 8 second shift


def scrape_game_pbp(season, game, force_overwrite=False):
    """
    This method scrapes the pbp for the given game. It formats it nicely and saves in a compressed format to disk.
    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If file exists already, won't scrape again
    :return: bool, False if not scraped, else True
    """
    filename = ss.get_game_raw_pbp_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    # Use the season schedule file to get the home and road team names
    # schedule_item = ss.get_season_schedule(season) \
    #    .query('Game == {0:d}'.format(game)) \
    #    .to_dict(orient = 'series')
    # The output format of above was {colname: np.array[vals]}. Change to {colname: val}
    # schedule_item = {k: v.values[0] for k, v in schedule_item.items()}

    page = ss.get_game_from_url(season, game)
    save_raw_pbp(page, season, game)
    ed.print_and_log('Scraped pbp for {0:d} {1:d}'.format(season, game))
    sleep(1)  # Don't want to overload NHL servers

    # It's most efficient to parse with page in memory, but for sake of simplicity will do it later
    # pbp = read_pbp_events_from_page(page)
    # update_team_logs(pbp, season, schedule_item['Home'])
    return True


def scrape_game_toi(season, game, force_overwrite=False):
    """
    This method scrapes the toi for the given game. It formats it nicely and saves in a compressed format to disk.
    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If file exists already, won't scrape again
    :return: nothing
    """
    filename = ss.get_game_raw_toi_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    url = ss.get_shift_url(season, game)
    with urllib.request.urlopen(url) as reader:
        page = reader.read()
    save_raw_toi(page, season, game)
    ed.print_and_log('Scraped toi for {0:d} {1:d}'.format(season, game))
    sleep(1)  # Don't want to overload NHL servers

    # It's most efficient to parse with page in memory, but for sake of simplicity will do it later
    # toi = read_toi_from_page(page)
    return True


def save_raw_pbp(page, season, game):
    """
    Takes the bytes page containing pbp information and saves to disk as a compressed zlib.
    :param page: bytes. str(page) would yield a string version of the json pbp
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """
    page2 = zlib.compress(page, level=9)
    filename = ss.get_game_raw_pbp_filename(season, game)
    w = open(filename, 'wb')
    w.write(page2)
    w.close()


def save_parsed_pbp(pbp, season, game):
    """
    Saves the pandas dataframe containing pbp information to disk as an HDF5.
    :param pbp: df, a pandas dataframe with the pbp of the game
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """
    pbp.to_hdf(ss.get_game_parsed_pbp_filename(season, game),
               key='P{0:d}0{1:d}'.format(season, game),
               mode='w', complib='zlib')


def save_parsed_toi(toi, season, game):
    """
    Saves the pandas dataframe containing shift information to disk as an HDF5.
    :param toi: df, a pandas dataframe with the shifts of the game
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """
    toi.to_hdf(ss.get_game_parsed_toi_filename(season, game),
               key='T{0:d}0{1:d}'.format(season, game),
               mode='w', complib='zlib')


def save_raw_toi(page, season, game):
    """
    Takes the bytes page containing shift information and saves to disk as a compressed zlib.
    :param page: bytes. str(page) would yield a string version of the json shifts
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """
    page2 = zlib.compress(page, level=9)
    filename = ss.get_game_raw_toi_filename(season, game)
    w = open(filename, 'wb')
    w.write(page2)
    w.close()


def get_raw_pbp(season, game):
    """
    Loads the compressed json file containing this game's play by play from disk.
    :param season: int, the season
    :param game: int, the game
    :return: json, the json pbp
    """
    with open(ss.get_game_raw_pbp_filename(season, game), 'rb') as reader:
        page = reader.read()
    return json.loads(str(zlib.decompress(page).decode('latin-1')))


def get_raw_toi(season, game):
    """
    Loads the compressed json file containing this game's shifts from disk.
    :param season: int, the season
    :param game: int, the game
    :return: json, the json shifts
    """
    with open(ss.get_game_raw_toi_filename(season, game), 'rb') as reader:
        page = reader.read()
    return json.loads(str(zlib.decompress(page).decode('latin-1')))


def get_parsed_pbp(season, game):
    """
    Loads the compressed json file containing this game's play by play from disk.
    :param season: int, the season
    :param game: int, the game
    :return: json, the json pbp
    """
    return pd.read_hdf(ss.get_game_parsed_pbp_filename(season, game))


def get_parsed_toi(season, game):
    """
    Loads the compressed json file containing this game's shifts from disk.
    :param season: int, the season
    :param game: int, the game
    :return: json, the json shifts
    """
    return pd.read_hdf(ss.get_game_parsed_toi_filename(season, game))


def update_team_logs(season, force_overwrite=False):
    """
    This method looks at the schedule for the given season and writes pbp for scraped games to file.
    It also adds the strength at each pbp event to the log.
    :param season: int, the season
    :param force_overwrite: bool, whether to generate from scratch
    :return:
    """

    # For each team

    spinner = halo.Halo()

    new_games_to_do = ss.get_season_schedule(season) \
        .query('PBPStatus == "Scraped" & TOIStatus == "Scraped"')
    allteams = list(new_games_to_do.Home.append(new_games_to_do.Road).unique())

    for teami, team in enumerate(allteams):
        spinner.start(text='Updating team log for {0:d} {1:s}\n'.format(season, ss.team_as_str(team)))

        # Compare existing log to schedule to find missing games
        newgames = new_games_to_do[(new_games_to_do.Home == team) | (new_games_to_do.Road == team)]
        if force_overwrite:
            pbpdf = None
            toidf = None
        else:
            # Read currently existing ones for each team and anti join to schedule to find missing games
            try:
                pbpdf = ss.get_team_pbp(season, team)
                newgames = newgames.merge(pbpdf[['Game']].drop_duplicates(), how='outer', on='Game', indicator=True)
                newgames = newgames[newgames._merge == "left_only"].drop('_merge', axis=1)
            except FileNotFoundError:
                pbpdf = None
            except pyarrow.lib.ArrowIOError:  # pyarrow (feather) FileNotFoundError equivalent
                pbpdf = None

            try:
                toidf = ss.get_team_toi(season, team)
            except FileNotFoundError:
                toidf = None
            except pyarrow.lib.ArrowIOError:  # pyarrow (feather) FileNotFoundError equivalent
                toidf = None

        for i, gamerow in newgames.iterrows():
            game = gamerow[1]
            home = gamerow[2]
            road = gamerow[4]

            # load parsed pbp and toi
            try:
                gamepbp = get_parsed_pbp(season, game)
                gametoi = get_parsed_toi(season, game)
                # TODO 2016 20779 why does pbp have 0 rows?
                # Also check for other errors in parsing etc

                if len(gamepbp) > 0 and len(gametoi) > 0:
                    # Rename score and strength columns from home/road to team/opp
                    if team == home:
                        gametoi = gametoi.assign(TeamStrength=gametoi.HomeStrength, OppStrength=gametoi.RoadStrength) \
                            .drop({'HomeStrength', 'RoadStrength'}, axis=1)
                        gamepbp = gamepbp.assign(TeamScore=gamepbp.HomeScore, OppScore=gamepbp.RoadScore) \
                            .drop({'HomeScore', 'RoadScore'}, axis=1)
                    else:
                        gametoi = gametoi.assign(TeamStrength=gametoi.RoadStrength, OppStrength=gametoi.HomeStrength) \
                            .drop({'HomeStrength', 'RoadStrength'}, axis=1)
                        gamepbp = gamepbp.assign(TeamScore=gamepbp.RoadScore, OppScore=gamepbp.HomeScore) \
                            .drop({'HomeScore', 'RoadScore'}, axis=1)

                    # add scores to toi and strengths to pbp
                    gamepbp = gamepbp.merge(gametoi[['Time', 'TeamStrength', 'OppStrength']], how='left', on='Time')
                    gametoi = gametoi.merge(gamepbp[['Time', 'TeamScore', 'OppScore']], how='left', on='Time')
                    gametoi.loc[:, 'TeamScore'] = gametoi.TeamScore.fillna(method='ffill')
                    gametoi.loc[:, 'OppScore'] = gametoi.OppScore.fillna(method='ffill')

                    # Switch TOI column labeling from H1/R1 to Team1/Opp1 as appropriate
                    cols_to_change = list(gametoi.columns)
                    cols_to_change = [x for x in cols_to_change if len(x) == 2]  # e.g. H1
                    if team == home:
                        swapping_dict = {'H': 'Team', 'R': 'Opp'}
                        colchanges = {c: swapping_dict[c[0]] + c[1] for c in cols_to_change}
                    else:
                        swapping_dict = {'H': 'Opp', 'R': 'Team'}
                        colchanges = {c: swapping_dict[c[0]] + c[1] for c in cols_to_change}
                    gametoi = gametoi.rename(columns=colchanges)

                    # finally, add game, home, and road to both dfs
                    gamepbp.loc[:, 'Game'] = game
                    gamepbp.loc[:, 'Home'] = home
                    gamepbp.loc[:, 'Road'] = road
                    gametoi.loc[:, 'Game'] = game
                    gametoi.loc[:, 'Home'] = home
                    gametoi.loc[:, 'Road'] = road

                    # concat toi and pbp
                    if pbpdf is None:
                        pbpdf = gamepbp
                    else:
                        pbpdf = pd.concat([pbpdf, gamepbp])
                    if toidf is None:
                        toidf = gametoi
                    else:
                        toidf = pd.concat([toidf, gametoi])

            except FileNotFoundError:
                pass

        # write to file
        if pbpdf is not None:
            pbpdf.loc[:, 'FocusTeam'] = team
        if toidf is not None:
            toidf.loc[:, 'FocusTeam'] = team

        ss.write_team_pbp(pbpdf, season, team)
        ss.write_team_toi(toidf, season, team)
        ed.print_and_log('Done with team logs for {0:d} {1:s} ({2:d}/{3:d})'.format(
            season, ss.team_as_str(team), teami + 1, len(allteams)), print_and_log=False)
        spinner.stop()
    ed.print_and_log('Updated team logs for {0:d}'.format(season))


def update_player_logs_from_page(pbp, season, game):
    """
    Takes the game play by play and adds players to the master player log file, noting that they were on the roster
    for this game, which team they played for, and their status (P for played, S for scratch).
    :param season: int, the season
    :param game: int, the game
    :param pbp: json, the pbp of the game
    :return: nothing
    """

    # Get players who played, and scratches, from boxscore
    home_played = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'players')
    road_played = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'players')
    home_scratches = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'scratches')
    road_scratches = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'scratches')

    # Played are both dicts, so make them lists
    home_played = [int(pid[2:]) for pid in home_played]
    road_played = [int(pid[2:]) for pid in road_played]

    # Played may include scratches, so make sure to remove them
    home_played = list(set(home_played).difference(set(home_scratches)))
    road_played = list(set(road_played).difference(set(road_scratches)))

    # Get home and road names
    gameinfo = ss.get_game_data_from_schedule(season, game)

    # Update player logs
    ss.update_player_log_file(home_played, season, game, gameinfo['Home'], 'P')
    ss.update_player_log_file(home_scratches, season, game, gameinfo['Home'], 'S')
    ss.update_player_log_file(road_played, season, game, gameinfo['Road'], 'P')
    ss.update_player_log_file(road_scratches, season, game, gameinfo['Road'], 'S')

    # TODO: One issue is we do not see goalies (and maybe skaters) who dressed but did not play. How can this be fixed?


def read_shifts_from_page(rawtoi, season, game):
    """
    
    :param rawtoi:
    :param season: int, the season
    :param game: int, the game
    :return: 
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
        ids[i] = ss.try_to_access_dict(dct, 'playerId', default_return='')
        periods[i] = ss.try_to_access_dict(dct, 'period', default_return=0)
        starts[i] = ss.try_to_access_dict(dct, 'startTime', default_return='0:00')
        ends[i] = ss.try_to_access_dict(dct, 'endTime', default_return='0:00')
        durations[i] = ss.try_to_access_dict(dct, 'duration', default_return=0)
        teams[i] = ss.try_to_access_dict(dct, 'teamId', default_return='')

    gameinfo = ss.get_game_data_from_schedule(season, game)

    # I originally took start times at face value and subtract 1 from end times
    # This caused problems with joining events--when there's a shot and the goalie freezes immediately
    # then, when you join this to the pbp, you'll get the players on the ice for the next draw as having
    # been on ice for the shot.
    # So I switch to adding 1 to start times, and leaving end times as-are.
    # That means that when joining on faceoffs, add 1 to faceoff times.
    # Exception: start time 1 --> start time 0
    startmin = [x[:x.index(':')] for x in starts]
    startsec = [x[x.index(':') + 1:] for x in starts]
    starttimes = [1200 * (p-1) + 60 * int(m) + int(s) + 1 for p, m, s in zip(periods, startmin, startsec)]
    starttimes = [0 if x == 1 else x for x in starttimes]
    endmin = [x[:x.index(':')] for x in ends]
    endsec = [x[x.index(':') + 1:] for x in ends]
    # There is an extra -1 in endtimes to avoid overlapping start/end
    endtimes = [1200 * (p - 1) + 60 * int(m) + int(s) for p, m, s in zip(periods, endmin, endsec)]

    durationtime = [e - s for s, e in zip(starttimes, endtimes)]

    df = pd.DataFrame({'PlayerID': ids, 'Period': periods, 'Start': starttimes, 'End': endtimes,
                       'Team': teams, 'Duration': durationtime})
    # TODO don't read end times. Use duration, which has good coverage, to infer end. Then end + 1200 not needed below.
    # Sometimes shifts have the same start and time.
    # By the time we're here, they'll have start = end + 1
    # So let's remove shifts with duration -1
    df = df[df.Start != df.End + 1]

    # Sometimes you see goalies with a shift starting in one period and ending in another
    # This is to help in those cases.
    if sum(df.End < df.Start) > 0:
        ed.print_and_log('Have to adjust a shift time', 'warn')
        # TODO I think I'm making a mistake with overtime shifts--end at 3900!
        ed.print_and_log(df[df.End < df.Start])
        df.loc[df.End < df.Start, 'End'] = df.End + 1200
    # One issue coming up is when the above line comes into play--missing times are filled in as 0:00
    tempdf = df[['PlayerID', 'Start', 'End', 'Team', 'Duration']].query("Duration > 0")
    tempdf = tempdf.assign(Time=tempdf.Start)
    # print(tempdf.head(20))

    # Let's filter out goalies for now. We can add them back in later.
    # This will make it easier to get the strength later
    pids = ss.get_player_ids_file()
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
        ed.print_and_log('Multiple goalies for a team in {0:d} {1:d}, picking one with the most TOI'.format(
            season, game), 'warn')

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
            problem_times_revised_h = problem_times_revised_h[problem_times_revised_h.ToDrop is not True] \
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
            problem_times_revised_r = problem_times_revised_r[problem_times_revised_r.ToDrop is not True] \
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
        ed.print_and_log('Some times from {0:d} {1:d} have too many home players; cutting off at 6'.format(
            season, game), 'warn')
        ed.print_and_log('Longest shift being lost was {0:d} seconds'.format(
            hdf[hdf['rank'] == "H7"].Duration.max()), 'warn')
    if len(rdf[rdf['rank'] == "R7"]) > 0:
        ed.print_and_log('Some times from {0:d} {1:d} have too many road players; cutting off at 6'.format(
            season, game), 'warn')
        ed.print_and_log('Longest shift being lost was {0:d} seconds'.format(
            rdf[rdf['rank'] == "H7"].Duration.max()), 'warn')

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
        ed.print_and_log('Dropped {0:d}/{1:d} times in {2:d} {3:d} because of invalid strengths'.format(
            len(toi) - len(toi2), len(toi), season, game), 'warn')

    # TODO data quality check that I don't miss times in the middle of the game

    return toi2


def read_events_from_page(rawpbp, season, game):
    """
    This method takes the json pbp and returns a pandas dataframe with the following columns:

    - Index: int, index of event
    - Period: str, period of event. In regular season, could be 1, 2, 3, OT, or SO. In playoffs, 1, 2, 3, 4, 5...
    - MinSec: str, m:ss, time elapsed in period
    - Time: int, time elapsed in game
    - Event: str, the event name
    - Team: int, the team id
    - Actor: int, the acting player id
    - ActorRole: str, e.g. for faceoffs there is a "Winner" and "Loser"
    - Recipient: int, the receiving player id
    - RecipientRole: str, e.g. for faceoffs there is a "Winner" and "Loser"
    - X: int, the x coordinate of event (or NaN)
    - Y: int, the y coordinate of event (or NaN)
    - Note: str, additional notes, which may include penalty duration, assists on a goal, etc.

    :param rawpbp: json, the raw json pbp
    :param season: int, the season
    :param game: int, the game
    :return: pandas dataframe, the pbp in a nicer format
    """
    pbp = ss.try_to_access_dict(rawpbp, 'liveData', 'plays', 'allPlays')
    if pbp is None:
        return

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
        period[i] = ss.try_to_access_dict(pbp, i, 'about', 'period', default_return='')
        times[i] = ss.try_to_access_dict(pbp, i, 'about', 'periodTime', default_return='0:00')
        event[i] = ss.try_to_access_dict(pbp, i, 'result', 'event', default_return='NA')

        xs[i] = float(ss.try_to_access_dict(pbp, i, 'coordinates', 'x', default_return=np.NaN))
        ys[i] = float(ss.try_to_access_dict(pbp, i, 'coordinates', 'y', default_return=np.NaN))
        team[i] = ss.try_to_access_dict(pbp, i, 'team', 'id', default_return=-1)

        p1[i] = ss.try_to_access_dict(pbp, i, 'players', 0, 'player', 'id', default_return=-1)
        p1role[i] = ss.try_to_access_dict(pbp, i, 'players', 0, 'playerType', default_return='')
        p2[i] = ss.try_to_access_dict(pbp, i, 'players', 1, 'player', 'id', default_return=-1)
        p2role[i] = ss.try_to_access_dict(pbp, i, 'players', 1, 'playerType', default_return='')

        note[i] = ss.try_to_access_dict(pbp, i, 'result', 'description', default_return='')

    pbpdf = pd.DataFrame({'Index': index, 'Period': period, 'MinSec': times, 'Event': event,
                          'Team': team, 'Actor': p1, 'ActorRole': p1role, 'Recipient': p2, 'RecipientRole': p2role,
                          'X': xs, 'Y': ys, 'Note': note})
    if len(pbpdf) == 0:
        return pbpdf

    # Add score
    gameinfo = ss.get_game_data_from_schedule(season, game)
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

    # Convert MM:SS and period to time in game
    minsec = pbpdf.MinSec.str.split(':', expand=True)
    minsec.columns = ['Min', 'Sec']
    minsec.Period = pbpdf.Period
    minsec.loc[:, 'Min'] = pd.to_numeric(minsec.loc[:, 'Min'])
    minsec.loc[:, 'Sec'] = pd.to_numeric(minsec.loc[:, 'Sec'])
    minsec.loc[:, 'TimeInPeriod'] = 60 * minsec.Min + minsec.Sec

    def period_contribution(x):
        try:
            return 1200 * (x-1)
        except ValueError:
            return 3600 if x == 'OT' else 3900  # OT or SO

    minsec.loc[:, 'PeriodContribution'] = minsec.Period.apply(period_contribution)
    minsec.loc[:, 'Time'] = minsec.PeriodContribution + minsec.TimeInPeriod
    pbpdf.loc[:, 'Time'] = minsec.Time

    return pbpdf


def update_player_ids_from_page(pbp):
    """
    Reads the list of players listed in the game file and adds to the player IDs file if they are not there already.
    :param pbp: json, the raw pbp
    :return: nothing
    """
    players = pbp['gameData']['players']  # yields the subdictionary with players
    ids = [key[2:] for key in players]  # keys are format "ID[PlayerID]"; pull that PlayerID part
    ss.update_player_ids_file(ids)


def parse_game_pbp(season, game, force_overwrite=False):
    """
    Reads the raw pbp from file, updates player IDs, updates player logs, and parses the JSON to a pandas DF
    and writes to file. Also updates team logs accordingly.
    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If True, will execute. If False, executes only if file does not exist yet.
    :return: True if parsed, False if not
    """

    filename = ss.get_game_parsed_pbp_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    # TODO for some earlier seasons I need to read HTML instead.
    # Looks like 2010-11 is the first year where this feed supplies more than just boxscore data
    rawpbp = get_raw_pbp(season, game)
    update_player_ids_from_page(rawpbp)
    update_player_logs_from_page(rawpbp, season, game)
    update_schedule_with_coaches(rawpbp, season, game)
    update_schedule_with_result(rawpbp, season, game)

    parsedpbp = read_events_from_page(rawpbp, season, game)
    save_parsed_pbp(parsedpbp, season, game)
    ed.print_and_log('Parsed events for {0:d} {1:d}'.format(season, game), print_and_log=False)
    return True


def update_schedule_with_result(pbp, season, game):
    """
    Uses the PbP to update results for this game.
    :param pbp: json, the pbp for this game
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """

    gameinfo = ss.get_game_data_from_schedule(season, game)
    result = None  # In case they have the same score. Like 2006 10009 has incomplete data, shows 0-0

    # If game is not final yet, don't do anything
    if gameinfo['Status'] != 'Final':
        return False

    # If one team one by at least two, we know it was a regulation win
    if gameinfo['HomeScore'] >= gameinfo['RoadScore'] + 2:
        result = 'W'
    elif gameinfo['RoadScore'] >= gameinfo['HomeScore'] + 2:
        result = 'L'
    else:
        # Check for the final period
        finalplayperiod = ss.try_to_access_dict(pbp, 'liveData', 'linescore', 'currentPeriodOrdinal')

        # Identify SO vs OT vs regulation
        if finalplayperiod is None:
            pass
        elif finalplayperiod == 'SO':
            if gameinfo['HomeScore'] > gameinfo['RoadScore']:
                result = 'SOW'
            elif gameinfo['RoadScore'] > gameinfo['HomeScore']:
                result = 'SOL'
        elif finalplayperiod[-2:] == 'OT':
            if gameinfo['HomeScore'] > gameinfo['RoadScore']:
                result = 'OTW'
            elif gameinfo['RoadScore'] > gameinfo['HomeScore']:
                result = 'OTL'
        else:
            if gameinfo['HomeScore'] > gameinfo['RoadScore']:
                result = 'W'
            elif gameinfo['RoadScore'] > gameinfo['HomeScore']:
                result = 'L'

    ss.update_schedule_with_result(season, game, result)


def update_schedule_with_coaches(pbp, season, game):
    """
    Uses the PbP to update coach info for this game.
    :param pbp: json, the pbp for this game
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """

    homecoach = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'coaches', 0, 'person', 'fullName')
    roadcoach = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'coaches', 0, 'person', 'fullName')
    ss.update_schedule_with_coaches(season, game, homecoach, roadcoach)


def parse_game_toi(season, game, force_overwrite=False):
    """

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If True, will execute. If False, executes only if file does not exist yet.
    :return: nothing
    """
    filename = ss.get_game_parsed_toi_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    # TODO for some earlier seasons I need to read HTML instead.
    # Looks like 2010-11 is the first year where this feed supplies more than just boxscore data
    rawtoi = get_raw_toi(season, game)
    try:
        parsedtoi = read_shifts_from_page(rawtoi, season, game)
    except ValueError as ve:
        ed.print_and_log('Error with {0:d} {1:d}'.format(season, game), 'warning')
        ed.print_and_log(str(ve), 'warning')  # TODO look through 2016, getting some errors
        parsedtoi = None

    if parsedtoi is None:
        return False

    # PbP doesn't have strengths, so let's add those in
    # Ok maybe leave strengths, scores, etc, for team logs
    # update_pbp_from_toi(parsedtoi, season, game)
    save_parsed_toi(parsedtoi, season, game)
    ed.print_and_log('Parsed shifts for {0:d} {1:d}'.format(season, game))
    return True

    # TODO


def _intervals(lst, interval_pct=10):
    """
    A method that divides list into intervals and returns tuples indicating each interval mark.
    Useful for giving updates when cycling through games.
    :param lst: lst to divide
    :param interval_pct: int, pct for each interval to represent. e.g. 10 means it will mark every 10%.
    :return: a list of tuples of (index, value)
    """

    lst = sorted(lst)
    intervals = []
    i = 0
    while True:
        frac = interval_pct / 100 * i
        index = round(len(lst) * frac)
        if index >= len(lst):
            break
        val = lst[index]
        intervals.append((index, val))
        i += 1
    return intervals


def parse_season_pbp(season, force_overwrite=False):
    """
    Parses pbp from the given season.
    :param season: int, the season
    :param force_overwrite: bool. If true, parses all games. If false, only previously unparsed ones
    :return:
    """
    spinner = halo.Halo(text='Parsing pbp from {0:d}'.format(season))
    spinner.start()
    if season is None:
        season = ss.get_current_season()

    sch = ss.get_season_schedule(season)
    games = sch.Game.values
    games.sort()
    intervals = _intervals(games)
    interval_j = 0
    for i, game in enumerate(games):
        try:
            parse_game_pbp(season, game, force_overwrite)
        except Exception as e:
            ed.print_and_log('{0:d} {1:d} {2:s}'.format(season, game, str(e)), 'warn')
        if interval_j < len(intervals):
            if i == intervals[interval_j][0]:
                spinner.start(text='Done parsing through {0:d} {1:d} ({2:d}%)'.format(
                    season, game, round(intervals[interval_j][0]/len(games) * 100)))
                interval_j += 1
    spinner.stop()


def parse_season_toi(season, force_overwrite=False):
    """
    Parses toi from the given season. Final games covered only.
    :param season: int, the season
    :param force_overwrite: bool. If true, parses all games. If false, only previously unparsed ones
    :return:
    """

    spinner = halo.Halo(text='Parsing toi from {0:d}'.format(season))
    if season is None:
        season = ss.get_current_season()

    sch = ss.get_season_schedule(season)
    games = sch[sch.Status == "Final"].Game.values
    games.sort()
    for game in games:
        parse_game_toi(season, game, force_overwrite)
    spinner.stop()


def autoupdate(season=None):
    """
    Run this method to update local data. It reads the schedule file for given season and scrapes and parses
    previously unscraped games that have gone final or are in progress.
    :param season: int, the season. If None (default), will do current season
    :return: nothing
    """

    if season is None:
        season = ss.get_current_season()

    sch = ss.get_season_schedule(season)

    # First, for all games that were in progress during last scrape, scrape again and parse again
    # TODO check that this actually works!
    #with halo.Halo(text="Updating previously in-progress games\n"):
    if True:
        inprogress = sch.query('Status == "In Progress"')
        inprogressgames = inprogress.Game.values
        inprogressgames.sort()
        for game in inprogressgames:
            scrape_game_pbp(season, game, True)
            scrape_game_toi(season, game, True)
            parse_game_pbp(season, game, True)
            parse_game_toi(season, game, True)
            ed.print_and_log('Done with {0:d} {1:d} (previously in progress)'.format(season, game))

    # Update schedule to get current status
    ss.generate_season_schedule_file(season)
    ss.refresh_schedules()
    sch = ss.get_season_schedule(season)

    # Now, for games currently in progress, scrape.
    # But no need to force-overwrite. We handled games previously in progress above.
    # Games newly in progress will be written to file here.
    with halo.Halo(text="Updating newly in-progress games\n"):
        inprogressgames = sch.query('Status == "In Progress"')
        inprogressgames = inprogressgames.Game.values
        inprogressgames.sort()
        for game in inprogressgames:
            scrape_game_pbp(season, game, False)
            scrape_game_toi(season, game, False)
            parse_game_pbp(season, game, False)
            parse_game_toi(season, game, False)
            ed.print_and_log('Done with {0:d} {1:d} (in progress)'.format(season, game))

    # Now, for any games that are final, run scrape_game, but don't necessarily force_overwrite
    with halo.Halo(text='Updating final games\n'):
        games = sch.query('Status == "Final"')
        games = games.Game.values
        games.sort()
        for game in games:
            gotpbp = False
            gottoi = False
            try:
                gotpbp = scrape_game_pbp(season, game, False)
                ss.update_schedule_with_pbp_scrape(season, game)
                parse_game_pbp(season, game, False)
            except urllib.error.HTTPError as he:
                ed.print_and_log('Could not access pbp url for {0:d} {1:d}'.format(season, game), 'warn')
                ed.print_and_log(str(he), 'warn')
            except urllib.error.URLError as ue:
                ed.print_and_log('Could not access pbp url for {0:d} {1:d}'.format(season, game), 'warn')
                ed.print_and_log(str(ue), 'warn')
            except Exception as e:
                ed.print_and_log(str(e), 'warn')
            try:
                gottoi = scrape_game_toi(season, game, False)
                ss.update_schedule_with_toi_scrape(season, game)
                parse_game_toi(season, game, False)
            except urllib.error.HTTPError as he:
                ed.print_and_log('Could not access toi url for {0:d} {1:d}'.format(season, game), 'warn')
                ed.print_and_log(str(he), 'warn')
            except urllib.error.URLError as ue:
                ed.print_and_log('Could not access toi url for {0:d} {1:d}'.format(season, game), 'warn')
                ed.print_and_log(str(ue), 'warn')
            except Exception as e:
                ed.print_and_log(str(e), 'warn')

            if gotpbp or gottoi:
                ed.print_and_log('Done with {0:d} {1:d} (final)'.format(season, game), print_and_log=False)

    try:
        update_team_logs(season, force_overwrite=False)
    except Exception as e:
        ed.print_and_log("Error with team logs in {0:d}: {1:s}".format(season, str(e)), 'warn')


if __name__ == "__main__":
    autoupdate()
    #for season in range(2010, 2017):
    #    update_team_logs(season, True)

    # autoupdate(2017)