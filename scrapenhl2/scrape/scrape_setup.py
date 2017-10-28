"""
This module contains several helpful methods for accessing files.
At import, this module creates folders for data storage if need be.

It also creates a team ID mapping and schedule files from 2005 through the current season (if the files do not exist).
"""

import json
import os
import os.path
import os.path
import urllib.error
import urllib.request

import pandas as pd
import scrapenhl2.scrape.get_files as get_files

import scrapenhl2.scrape.get_filenames as get_filenames
import scrapenhl2.scrape.get_urls as get_urls


def _create_folders_and_files():
    """
    Creates folders for data storage if needed:

    - /scrape/data/raw/pbp/[seasons]/
    - /scrape/data/raw/toi/[seasons]/
    - /scrape/data/parsed/pbp/[seasons]/
    - /scrape/data/parsed/toi/[seasons]/
    - /scrape/data/teams/pbp/[seasons]/
    - /scrape/data/teams/toi/[seasons]/
    - /scrape/data/other/

    Also creates team IDs file and season schedule files if they don't exist already.
    :return: nothing
    """
    # ------- Raw -------
    for season in range(2005, get_files.get_current_season() + 1):
        get_filenames.check_create_folder(get_filenames.get_season_raw_pbp_folder(season))
    for season in range(2005, get_files.get_current_season() + 1):
        get_filenames.check_create_folder(get_filenames.get_season_raw_toi_folder(season))

    # ------- Parsed -------
    for season in range(2005, get_files.get_current_season() + 1):
        get_filenames.check_create_folder(get_filenames.get_season_parsed_pbp_folder(season))
    for season in range(2005, get_files.get_current_season() + 1):
        get_filenames.check_create_folder(get_filenames.get_season_parsed_toi_folder(season))

    # ------- Team logs -------
    for season in range(2005, get_files.get_current_season() + 1):
        get_filenames.check_create_folder(get_filenames.get_season_team_pbp_folder(season))
    for season in range(2005, get_files.get_current_season() + 1):
        get_filenames.check_create_folder(get_filenames.get_season_team_toi_folder(season))

    # ------- Other stuff -------
    get_filenames.check_create_folder(get_filenames.get_other_data_folder())

    if not os.path.exists(get_filenames.get_team_info_filename()):
        generate_team_ids_file()  # team IDs file

    for season in range(2005, get_files.get_current_season() + 1):
        if not os.path.exists(get_filenames.get_season_schedule_filename(season)):
            generate_season_schedule_file(season)  # season schedule
        # There is a potential issue here for current season.
        # For current season, we'll update this as we go along.
        # But original creation first time you start up in a new season is automatic, here.
        # When we autoupdate season date, we need to make sure to re-access this file and add in new entries

    if not os.path.exists(get_filenames.get_player_ids_filename()):
        generate_player_ids_file()

    if not os.path.exists(get_filenames.get_player_log_filename()):
        generate_player_log_file()






# @ed.once_per_second
def get_game_from_url(season, game):
    """
    Gets the page containing information for specified game from NHL API.
    :param season: int, the season
    :param game: int, the game
    :return: str, the page at the url
    """
    url = get_urls.get_game_url(season, game)
    with urllib.request.urlopen(url) as reader:
        page = reader.read()
    return page







def generate_player_ids_file():
    """
    Creates a dataframe with these columns:

    - ID: int, player ID
    - Name: str, player name
    - DOB: str, date of birth
    - Hand: char, R or L
    - Pos: char, one of C/R/L/D/G

    It will be populated with Alex Ovechkin to start.
    :return: nothing
    """
    df = pd.DataFrame({'ID': [8471214],
                       'Name': ['Alex Ovechkin'],
                       'DOB': ['1985-09-17'],
                       'Hand': ['R'],
                       'Pos': ['L'],
                       'Height': ["6'3\""],
                       'Weight': [235],
                       'Nationality': ['RUS']})
    write_player_ids_file(df)


def generate_player_log_file():
    """
    Run this when no player log file exists already. This is for getting the datatypes right. Adds Alex Ovechkin
    in Game 1 vs Pittsburgh in 2016-2017.
    :return: nothing
    """
    df = pd.DataFrame({'ID': [8471214],  # Player ID (Ovi)
                       'Team': [15],  # Team (WSH)
                       'Status': ['P'],  # P for played, S for scratch.  # TODO can I do healthy vs injured?
                       'Season': [2016],  # Season (2016-17)
                       'Game': [30221]})  # Game (G1 vs PIT)
    if os.path.exists(get_filenames.get_player_log_filename()):
        pass  # ed.print_and_log('Warning: overwriting existing player log with default, one-line df!', 'warn')
    get_files.write_player_log_file(df)

def get_teams_in_season(season):
    """
    Returns all teams that have a game in the schedule for this season
    :param season: int, the season
    :return: set of team IDs
    """

    sch = get_files.get_season_schedule(season)
    allteams = set(sch.Road).union(sch.Home)
    return set(allteams)



def get_player_info_from_url(playerid):
    """
    Gets ID, Name, Hand, Pos, DOB, Height, Weight, and Nationality from the NHL API.
    :param playerid: int, the player id
    :return: dict with player ID, name, handedness, position, etc
    """
    with urllib.request.urlopen(get_urls.get_player_url(playerid)) as reader:
        page = reader.read().decode('latin-1')
    data = json.loads(page)

    info = {}
    vars_to_get = {'ID': ['people', 0, 'id'],
                   'Name': ['people', 0, 'fullName'],
                   'Hand': ['people', 0, 'shootsCatches'],
                   'Pos': ['people', 0, 'primaryPosition', 'code'],
                   'DOB': ['people', 0, 'birthDate'],
                   'Height': ['people', 0, 'height'],
                   'Weight': ['people', 0, 'weight'],
                   'Nationality': ['people', 0, 'nationality']}
    for key, val in vars_to_get.items():
        info[key] = try_to_access_dict(data, *val)

    # Remove the space in the middle of height
    if info['Height'] is not None:
        info['Height'] = info['Height'].replace(' ', '')
    return info








def delete_game_html(season, game):
    """
    Deletes html files. HTML files are used for live game charts, but deleted in favor of JSONs when games go final.
    :param season: int, the season
    :param game: int, the game
    :return: nothing
    """

    for fun in (get_filenames.get_game_pbplog_filename,
                get_filenames.get_home_shiftlog_filename,
                get_filenames.get_road_shiftlog_filename):
        filename = fun(season, game)
        if os.path.exists(filename):
            os.remove(filename)





def setup():
    """
    Loads current season, base directory, etc. Always run this method first!
    :return: nothing
    """
    get_files.setup()
    global _EVENT_DICT, _TEAM_COLORS


    _create_folders_and_files()
    _EVENT_DICT = _get_event_dictionary()


setup()
