import os
import os.path
import feather
import pandas as pd
import json

BASE_DIR = os.getcwd()

import datetime
CURRENT_SEASON = datetime.datetime.now().year - 1
if datetime.datetime.now().month >= 9:
    CURRENT_SEASON += 1

def get_base_dir():
    return BASE_DIR


def check_create_folder(*args):
    path = os.path.join(get_base_dir(), *args)
    if not os.path.exists(path):
        os.makedirs(path)

def create_folders_and_files():
    ### ------- Raw -------
    for season in range(2005, CURRENT_SEASON + 1):
        check_create_folder(get_season_raw_pbp_folder(season))
    for season in range(2005, CURRENT_SEASON + 1):
        check_create_folder(get_season_raw_toi_folder(season))

    ### ------- Parsed -------
    for season in range(2005, CURRENT_SEASON + 1):
        check_create_folder(get_season_parsed_pbp_folder(season))
    for season in range(2005, CURRENT_SEASON + 1):
        check_create_folder(get_season_parsed_toi_folder(season))

    ### ------- Team logs -------
    for season in range(2005, CURRENT_SEASON + 1):
        check_create_folder(get_season_team_pbp_folder(season))
    for season in range(2005, CURRENT_SEASON + 1):
        check_create_folder(get_season_team_toi_folder(season))

    ### ------- Other stuff -------
    check_create_folder(get_other_data_folder())

    if not os.path.exists(get_team_info_filename()):
        generate_team_ids_file() #team IDs file

    for season in range(2005, CURRENT_SEASON + 1):
        if not os.path.exists(get_season_schedule_filename(season)):
            generate_season_schedule_file(season) #season schedule
        # There is a potential issue here for current season.
        # For current season, we'll update this as we go along.
        # But original creation first time you start up in a new season is automatic, here.
        # When we autoupdate season date, we need to make sure to re-access this file and add in new entries



def get_game_raw_pbp_filename(season, game):
    """ /scrape/data/raw/pbp/[season]/[game].JSON """
    return os.path.join(get_season_raw_pbp_folder(season), str(game) + '.json')
def get_game_raw_toi_filename(season, game):
    """ /scrape/data/raw/toi/[season]/[game].JSON """
    return os.path.join(get_season_raw_toi_folder(season), str(game) + '.json')
def get_game_parsed_pbp_filename(season, game):
    """ /scrape/data/parsed/pbp/[season]/[game].zlib """
    return os.path.join(get_season_parsed_pbp_folder(season), str(game) + '.zlib')
def get_game_parsed_toi_filename(season, game):
    """/scrape/data/parsed/toi/[season]/[game].zlib """
    return os.path.join(get_season_parsed_toi_folder(season), str(game) + '.zlib')

def get_raw_data_folder():
    return os.path.join('scrape', 'data', 'raw')
def get_parsed_data_folder():
    return os.path.join('scrape', 'data', 'parsed')
def get_team_data_folder():
    return os.path.join('scrape', 'data', 'teams')
def get_other_data_folder():
    return os.path.join('scrape', 'data', 'other')

def get_season_raw_pbp_folder(season):
    return os.path.join(get_raw_data_folder(), 'pbp', str(season))
def get_season_raw_toi_folder(season):
    return os.path.join(get_raw_data_folder(), 'toi', str(season))
def get_season_parsed_pbp_folder(season):
    return os.path.join(get_parsed_data_folder(), 'pbp', str(season))
def get_season_parsed_toi_folder(season):
    return os.path.join(get_parsed_data_folder(), 'toi', str(season))

def get_season_team_pbp_folder(season):
    return os.path.join(get_team_data_folder(), 'pbp', str(season))
def get_season_team_toi_folder(season):
    return os.path.join(get_team_data_folder(), 'toi', str(season))

def get_team_info_filename():
    return os.path.join(get_other_data_folder(), 'TEAM_INFO.feather')
def get_team_info_file():
    return feather.read_dataframe(get_team_info_filename())
def write_team_info_file(df):
    feather.write_dataframe(df, get_team_info_filename())
def get_team_info_url(teamid):
    return 'http://statsapi.web.nhl.com/api/v1/teams/{0:d}'.format(teamid)
def generate_team_ids_file(limit=110):
    print('Creating team IDs file')
    import urllib.request
    ids = []
    abbrevs = []
    names = []

    for i in range(1, limit + 1):
        url = get_team_info_url(i)
        try:
            with urllib.request.urlopen(url) as reader:
                page = reader.read()
            teaminfo = json.loads(page.decode('latin-1'))

            tid = teaminfo['teams'][0]['id']
            tabbrev = teaminfo['teams'][0]['abbreviation']
            tname = teaminfo['teams'][0]['name']

            ids.append(tid)
            abbrevs.append(tabbrev)
            names.append(tname)

            print('Done with ID #', tid, ':', tname)

        except urllib.error.HTTPError:
            pass

    teaminfo = pd.DataFrame({'ID': ids, 'Abbreviation': abbrevs, 'Name': names})
    write_team_info_file(teaminfo)
    print('Done writing team IDs')

def get_season_schedule_url(season):
    return 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=' \
           '{0:d}-09-01&endDate={1:d}-06-25'.format(season, season + 1)
def get_season_schedule_filename(season):
    return os.path.join(get_other_data_folder(), '{0:d}_schedule.feather'.format(season))
def get_season_schedule(season):
    return feather.read_dataframe(get_season_schedule_filename(season))

def try_to_access_dict(base_dct, *keys):
    temp = base_dct
    try:
        for key in keys:
            temp = temp[key]
        return temp
    except KeyError:
        return None

def generate_season_schedule_file(season, force_overwrite=True):
    print('Generating season schedule for', season)
    import urllib.request
    url = get_season_schedule_url(season)
    with urllib.request.urlopen(url) as reader:
        page = reader.read()

    dates = []
    games = []
    gametypes = []
    statuses = []
    vids = []
    vscores = []
    hids = []
    hscores = []
    venues = []

    page2 = json.loads(page)
    for datejson in page2['dates']:
        try:
            date = try_to_access_dict(datejson, 'date')
            for gamejson in datejson['games']:
                game = try_to_access_dict(gamejson, 'gamePk')
                gametype = try_to_access_dict(gamejson, 'gameType')
                status = try_to_access_dict(gamejson, 'status', 'detailedState')
                vid = try_to_access_dict(gamejson, 'teams', 'away', 'team', 'id')
                vscore = try_to_access_dict(gamejson, 'teams', 'away', 'score')
                hid = try_to_access_dict(gamejson, 'teams', 'home', 'team', 'id')
                hscore = try_to_access_dict(gamejson, 'teams', 'home', 'score')
                venue = try_to_access_dict(gamejson, 'venue', 'name')

                dates.append(date)
                games.append(game)
                gametypes.append(gametype)
                statuses.append(status)
                vids.append(vid)
                vscores.append(vscore)
                hids.append(hid)
                hscores.append(hscore)
                venues.append(venue)
        except KeyError:
            pass
    df = pd.DataFrame({'Date': dates,
                       'Game': games,
                       'Type': gametypes,
                       'Status': statuses,
                       'Road': vids,
                       'RoadScore': vscores,
                       'Home': hids,
                       'HomeScore': hscores,
                       'Venue': venues})
    df.loc[:, 'Season'] = season
    df.loc[:, 'HomeCoach'] = 'N/A'
    df.loc[:, 'RoadCoach'] = 'N/A'
    df.loc[:, 'Result'] = 'N/A'
    df.loc[:, 'PBPStatus'] = 'Not scraped'
    df.loc[:, 'TOIStatus'] = 'Not scraped'

    if force_overwrite: # Easy--just write it
        feather.write_dataframe(df, get_season_schedule_filename(season))
    else: # Only write new games/previously unfinished games
        olddf = feather.read_dataframe(get_season_schedule_filename(season))
        olddf = olddf.query('Status != "Final"')

        game_diff = set(df.Game).difference(olddf.Game)
        where_diff = df.Key.isin(game_diff)
        newdf = pd.concat(olddf, df[where_diff], ignore_index=True)

        feather.write_dataframe(newdf, get_season_schedule_filename(season))


    print('Done generating schedule for', season)



create_folders_and_files()


