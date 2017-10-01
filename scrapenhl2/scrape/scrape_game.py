import scrape_setup
import os
import os.path
import feather
import pandas as pd
import json
import urllib.request
import urllib.error
import datetime
import zlib
import numpy as np
import time

def scrape_game_pbp(season, game, force_overwrite=False):
    """
    This method scrapes the pbp for the given game. It formats it nicely and saves in a compressed format to disk.
    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If file exists already, won't scrape again
    :return: bool, False if not scraped, else True
    """
    filename = scrape_setup.get_game_raw_pbp_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    # Use the season schedule file to get the home and road team names
    # schedule_item = scrape_setup.get_season_schedule(season) \
    #    .query('Game == {0:d}'.format(game)) \
    #    .to_dict(orient = 'series')
    # The output format of above was {colname: np.array[vals]}. Change to {colname: val}
    # schedule_item = {k: v.values[0] for k, v in schedule_item.items()}

    url = scrape_setup.get_game_url(season, game)
    with urllib.request.urlopen(url) as reader:
        page = reader.read()
    save_raw_pbp(page, season, game)
    # time.sleep(1)

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
    filename = scrape_setup.get_game_raw_toi_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    # Use the season schedule file to get the home and road team names
    schedule_item = scrape_setup.get_season_schedule(season) \
        .query('Game == {0:d}'.format(game)) \
        .to_dict(orient='series')
    # The output format of above was {colname: np.array[vals]}. Change to {colname: val}
    schedule_item = {k: v.values[0] for k, v in schedule_item.items()}

    url = scrape_setup.get_shift_url(season, game)
    with urllib.request.urlopen(url) as reader:
        page = reader.read()
    save_raw_toi(page, season, game)
    # time.sleep(1)

    #toi = read_toi_from_page(page)
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
    filename = scrape_setup.get_game_raw_pbp_filename(season, game)
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
    pbp.to_hdf(scrape_setup.get_game_parsed_pbp_filename(season, game),
               key = 'P{0:d}0{1:d}'.format(season, game),
               mode='w', complib='zlib')


def save_parsed_toi(toi, season, game):
    """
    Saves the pandas dataframe containing shift information to disk as an HDF5.
    :param toi: df, a pandas dataframe with the shifts of the game
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """
    toi.to_hdf(scrape_setup.get_game_parsed_toi_filename(season, game),
               key = 'T{0:d}0{1:d}'.format(season, game),
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
    filename = scrape_setup.get_game_raw_toi_filename(season, game)
    w = open(filename, 'wb')
    w.write(page2)
    w.close()


def open_raw_pbp(season, game):
    """
    Loads the compressed json file containing this game's play by play from disk.
    :param season: int, the season
    :param game: int, the game
    :return: json, the json pbp
    """
    with open(scrape_setup.get_game_raw_pbp_filename(season, game), 'rb') as reader:
        page = reader.read()
    return json.loads(str(zlib.decompress(page).decode('latin-1')))


def open_raw_toi(season, game):
    """
    Loads the compressed json file containing this game's shifts from disk.
    :param season: int, the season
    :param game: int, the game
    :return: json, the json shifts
    """
    with open(scrape_setup.get_game_raw_toi_filename(season, game), 'rb') as reader:
        page = reader.read()
    return json.loads(str(zlib.decompress(page).decode('latin-1')))


def update_team_logs(newdata, season, team, perspective='same'):
    pass
def update_team_pbp(newdata, season, team, perspective='same'):
    pass
def update_team_toi(newdata, season, team, perspective='same'):
    pass
def update_player_logs(season, game, playerids):
    pass
def update_season_schedule_gamesscraped(season, game, status='Scraped'):
    pass
def read_shifts_from_json(data, homename = None, roadname = None):

    if len(data) == 0:
        return
    ids = ['' for i in range(len(data))]
    periods = [0 for i in range(len(data))]
    starts = ['0:00' for i in range(len(data))]
    ends = ['0:00' for i in range(len(data))]
    teams = ['' for i in range(len(data))]
    durations = [0 for i in range(len(data))]

    for i, dct in enumerate(data):
        ids[i] = dct['playerId']
        periods[i] = dct['period']
        starts[i] = dct['startTime']
        ends[i] = dct['endTime']
        durations[i] = dct['duration']
        teams[i] = dct['teamAbbrev']

    ### Seems like home players come first
    if homename is None:
        homename = teams[0]
        for i in range(len(teams) - 1, 0, -1):
            if not teams[i] == homename:
                roadname = teams[i]
                break

    startmin = [x[:x.index(':')] for x in starts]
    startsec = [x[x.index(':') + 1:] for x in starts]
    starttimes = [1200 * (p-1) + 60 * int(m) + int(s) for p, m, s in zip(periods, startmin, startsec)]
    endmin = [x[:x.index(':')] for x in ends]
    endsec = [x[x.index(':') + 1:] for x in ends]
    ### There is an extra -1 in endtimes to avoid overlapping start/end
    endtimes = [1200 * (p - 1) + 60 * int(m) + int(s) - 1 for p, m, s in zip(periods, endmin, endsec)]

    durationtime = [e - s for s, e in zip(starttimes, endtimes)]

    import pandas as pd
    df = pd.DataFrame({'PlayerID': ids, 'Period': periods, 'Start': starttimes, 'End': endtimes,
                       'Team': teams, 'Duration': durationtime})
    df.loc[df.End < df.Start, 'End'] = df.End + 1200
    tempdf = df[['PlayerID', 'Start', 'End', 'Team', 'Duration']]
    tempdf = tempdf.assign(Time = tempdf.Start)
    #print(tempdf.head(20))

    toi = pd.DataFrame({'Time': [i for i in range(0, max(df.End) + 1)]})

    toidfs = []
    while len(tempdf.index) > 0:
        temptoi = toi.merge(tempdf, how = 'inner', on = 'Time')
        toidfs.append(temptoi)

        tempdf = tempdf.assign(Time = tempdf.Time + 1)
        tempdf = tempdf.query('Time <= End')

    tempdf = pd.concat(toidfs)
    tempdf = tempdf.sort_values(by = 'Time')

    ### Append team name to start of columns by team
    hdf = tempdf.query('Team == "' + homename + '"')
    hdf2 = hdf.groupby('Time').rank()
    hdf2 = hdf2.rename(columns = {'PlayerID': 'rank'})
    hdf2.loc[:, 'rank'] = hdf2['rank'].apply(lambda x: int(x))
    hdf.loc[:, 'rank'] = homename + hdf2['rank'].astype('str')

    rdf = tempdf.query('Team == "' + roadname + '"')
    rdf2 = rdf.groupby('Time').rank()
    rdf2 = rdf2.rename(columns={'PlayerID': 'rank'})
    rdf2.loc[:, 'rank'] = rdf2['rank'].apply(lambda x: int(x))
    rdf.loc[:, 'rank'] = roadname + rdf2['rank'].astype('str')

    ### Occasionally bad entries make duplicates on time and rank. Take one with longer duration

    tokeep = hdf.sort_values(by = 'Duration', ascending = False)
    tokeep = tokeep.groupby(['Time', 'PlayerID']).first()
    tokeep.reset_index(inplace = True)
    hdf = hdf.merge(tokeep, how = 'inner', on = ['Time', 'PlayerID', 'Start', 'End', 'Team', 'rank'])

    tokeep = rdf.sort_values(by='Duration', ascending=False)
    tokeep = tokeep.groupby(['Time', 'PlayerID']).first()
    tokeep.reset_index(inplace=True)
    rdf = rdf.merge(tokeep, how='inner', on=['Time', 'PlayerID', 'Start', 'End', 'Team', 'rank'])

    ### Remove values above 6--looking like there won't be many
    ### TODO: keep goalie if one is a goalie!
    hdf = hdf.pivot(index = 'Time', columns = 'rank', values = 'PlayerID').iloc[:, 0:6]
    hdf.reset_index(inplace = True) #get time back as a column
    rdf = rdf.pivot(index='Time', columns='rank', values='PlayerID').iloc[:, 0:6]
    rdf.reset_index(inplace = True)

    toi = toi.merge(hdf, how = 'left', on = 'Time').merge(rdf, how = 'left', on = 'Time')

    return(toi)


def read_events_from_page(pbp):
    """
    Returns the NHL API url to scrape.
    Parameters
    -----------
    season : int
        The season of the game. 2007-08 would be 2007.
    game : int
        The game id. This can range from 20001 to 21230 for regular season, and 30111 to 30417 for playoffs.
        The preseason, all-star game, Olympics, and World Cup also have game IDs that can be provided.
    Returns
    --------
    pandas df
        Dataframe of the game's play by play data
    """

    index = [i for i in range(len(pbp))]
    period = [-1 for i in range(len(pbp))]
    time = ['0:00' for i in range(len(pbp))]
    event = ['NA' for i in range(len(pbp))]

    team = [-1 for i in range(len(pbp))]
    p1 = [-1 for i in range(len(pbp))]
    p1role = ['' for i in range(len(pbp))]
    p2 = [-1 for i in range(len(pbp))]
    p2role = ['' for i in range(len(pbp))]
    xy = [(np.NaN, np.NaN) for i in range(len(pbp))]
    note = ['' for i in range(len(pbp))]

    for i in range(len(pbp)):
        period[i] = int(pbp[i]['about']['period'])
        time[i] = pbp[i]['about']['periodTime']
        event[i] = pbp[i]['result']['event']

        try:
            xy[i] = (float(pbp[i]['coordinates']['x']), float(pbp[i]['coordinates']['y']))
        except KeyError:
            pass
        try:
            team[i] = pbp[i]['team']['id']
        except KeyError:
            pass
        try:
            p1[i] = pbp[i]['players'][0]['player']['id']
            p1role[i] = pbp[i]['players'][0]['playerType']
        except KeyError:
            pass
        try:
            p2[i] = pbp[i]['players'][1]['player']['id']
            p2role[i] = pbp[i]['players'][1]['playerType']
        except KeyError:
            pass
        except IndexError: #e.g. on a give or take
            pass

        try:
            note[i] = pbp[i]['result']['description']
        except KeyError:
            pass

        #print(period[i], time[i], event[i], xy[i], team[i], p1[i], p1role[i], p2[i], p2role[i])

    pbpdf = pd.DataFrame({'Index': index, 'Period': period, 'Time': time, 'Event': event,
                          'Team': team, 'Actor': p1, 'ActorRole': p1role, 'Recipient': p2, 'RecipientRole': p2role,
                          'XY': xy, 'Note': note})
    return pbpdf


def update_player_ids_from_page(pbp):
    """

    :param pbp:
    :return:
    """
    players = pbp['gameData']['players'] # yields the subdictionary with players
    ids = [key[2:] for key in players] # keys are format "ID[PlayerID]"; pull that PlayerID part
    scrape_setup.update_player_ids_file(ids)

def parse_game_pbp(season, game, force_overwrite=False):
    """

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If True, will execute. If False, executes only if file does not exist yet.
    :return: True if parsed, False if not
    """
    filename = scrape_setup.get_game_raw_pbp_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    rawpbp = open_raw_pbp(season, game)
    update_player_ids_from_page(rawpbp)
    #parsedpbp = read_events_from_page(rawpbp)


def parse_game_toi(season, game, force_overwrite=False):
    """

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If True, will execute. If False, executes only if file does not exist yet.
    :return: nothing
    """
    pass


def autoupdate(season=None):
    """

    :param season: int, the season
    :return:
    """
    if season is None:
        season = scrape_setup.get_current_season()

    sch = scrape_setup.get_season_schedule(season)

    # First, for all games that were in progress during last scrape, scrape again and parse again
    inprogress = sch.query('Status == "In Progress"')
    inprogressgames = inprogress.Game.values
    for game in inprogressgames:
        scrape_game_pbp(season, game, True)
        scrape_game_toi(season, game, True)
        parse_game_pbp(season, game, True)
        parse_game_toi(season, game, True)
        print('Done with', season, game)

    # Update schedule to get current status
    scrape_setup.generate_season_schedule_file(season)
    scrape_setup.refresh_schedules()
    sch = scrape_setup.get_season_schedule(season)

    # Now, for any games that are final, run scrape_game, but don't force_overwrite
    games = sch.query('Status == "Final"')
    games = games.Game.values
    for game in games:
        _ = scrape_game_pbp(season, game, False)
        parse_game_pbp(season, game, True)
        scrape_setup._update_schedule_with_pbp_scrape(season, game)
        _ = scrape_game_toi(season, game, False)
        parse_game_toi(season, game, True)
        scrape_setup._update_schedule_with_toi_scrape(season, game)

        print('Done with', season, game)

    # TODO: Also parse games


autoupdate(2016)