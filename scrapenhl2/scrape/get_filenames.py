"""
This module contains methods for getting filenames.
"""

import os


def check_create_folder(*args):
    """
    A helper method to create a folder if it doesn't exist already
    :param args: the parts of the filepath. These are joined together with the base directory
    :return: nothing
    """
    path = os.path.join(get_base_dir(), *args)
    if not os.path.exists(path):
        os.makedirs(path)


def get_base_dir():
    """
    Returns the base directory of this package (one directory up from this file)
    :return: the base directory
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def get_game_raw_pbp_filename(season, game):
    """
    Returns the filename of the raw pbp folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/raw/pbp/[season]/[game].zlib
    """
    return os.path.join(get_season_raw_pbp_folder(season), str(game) + '.zlib')


def get_game_raw_toi_filename(season, game):
    """
    Returns the filename of the raw toi folder
    :param season: int, current season
    :param game: int, game
    :return:  /scrape/data/raw/toi/[season]/[game].zlib
    """
    return os.path.join(get_season_raw_toi_folder(season), str(game) + '.zlib')


def get_game_parsed_pbp_filename(season, game):
    """
    Returns the filename of the parsed pbp folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/parsed/pbp/[season]/[game].zlib
    """
    return os.path.join(get_season_parsed_pbp_folder(season), str(game) + '.h5')


def get_game_parsed_toi_filename(season, game):
    """
    Returns the filename of the parsed toi folder
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/parsed/toi/[season]/[game].zlib
    """
    return os.path.join(get_season_parsed_toi_folder(season), str(game) + '.h5')


def get_raw_data_folder():
    """
    Returns the folder containing raw data
    :return: /scrape/data/raw/
    """
    return os.path.join(get_base_dir(), 'data', 'raw')


def get_parsed_data_folder():
    """
    Returns the folder containing parsed data
    :return: /scrape/data/parsed/
    """
    return os.path.join(get_base_dir(), 'data', 'parsed')


def get_team_data_folder():
    """
    Returns the folder containing team log data
    :return: /scrape/data/teams/
    """
    return os.path.join(get_base_dir(), 'data', 'teams')


def get_other_data_folder():
    """
    Returns the folder containing other data
    :return: /scrape/data/other/
    """
    return os.path.join(get_base_dir(), 'data', 'other')


def get_season_raw_pbp_folder(season):
    """
    Returns the folder containing raw pbp for given season
    :param season: int, current season
    :return: /scrape/data/raw/pbp/[season]/
    """
    return os.path.join(get_raw_data_folder(), 'pbp', str(season))


def get_season_raw_toi_folder(season):
    """
    Returns the folder containing raw toi for given season
    :param season: int, current season
    :return: /scrape/data/raw/toi/[season]/
    """
    return os.path.join(get_raw_data_folder(), 'toi', str(season))


def get_season_parsed_pbp_folder(season):
    """
    Returns the folder containing parsed pbp for given season
    :param season: int, current season
    :return: /scrape/data/parsed/pbp/[season]/
    """
    return os.path.join(get_parsed_data_folder(), 'pbp', str(season))


def get_season_parsed_toi_folder(season):
    """
    Returns the folder containing parsed toi for given season
    :param season: int, current season
    :return: /scrape/data/raw/toi/[season]/
    """
    return os.path.join(get_parsed_data_folder(), 'toi', str(season))


def get_season_team_pbp_folder(season):
    """
    Returns the folder containing team pbp logs for given season
    :param season: int, current season
    :return: /scrape/data/teams/pbp/[season]/
    """
    return os.path.join(get_team_data_folder(), 'pbp', str(season))


def get_season_team_toi_folder(season):
    """
    Returns the folder containing team toi logs for given season
    :param season: int, current season
    :return: /scrape/data/teams/toi/[season]/
    """
    return os.path.join(get_team_data_folder(), 'toi', str(season))


def get_team_pbp_filename(season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    return os.path.join(get_season_team_pbp_folder(season),
                        "{0:s}.feather".format(team_as_str(team, abbreviation=True)))


def get_team_toi_filename(season, team):
    """

    :param season: int, the season
    :param team: int or str, the team abbreviation.
    :return:
    """
    return os.path.join(get_season_team_toi_folder(season),
                        "{0:s}.feather".format(team_as_str(team, abbreviation=True)))


def get_team_info_filename():
    """
    Returns the team information filename
    :return: /scrape/data/other/TEAM_INFO.feather
    """
    return os.path.join(get_other_data_folder(), 'TEAM_INFO.feather')


def get_player_log_filename():
    """
    Returns the player log filename.
    :return: str, /scrape/data/other/PLAYER_LOG.feather
    """
    return os.path.join(get_other_data_folder(), 'PLAYER_LOG.feather')


def get_player_5v5_log_filename(season):
    """
    Gets the filename for the season's player log file. Includes 5v5 CF, CA, TOI, and more.
    :param season: int, the season
    :return: /scrape/data/other/[season]_player_log.feather
    """
    return os.path.join(get_other_data_folder(), '{0:d}_player_5v5_log.feather'.format(season))


def get_season_schedule_filename(season):
    """
    Gets the filename for the season's schedule file
    :param season: int, the season
    :return: /scrape/data/other/[season]_schedule.feather
    """
    return os.path.join(get_other_data_folder(), '{0:d}_schedule.feather'.format(season))


def get_game_pbplog_filename(season, game):
    """
    Returns the filename of the parsed pbp html game pbp
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/raw/pbp/[season]/[game].html
    """
    return os.path.join(get_season_raw_pbp_folder(season), str(game) + '.html')


def get_home_shiftlog_filename(season, game):
    """
    Returns the filename of the parsed toi html home shifts
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/raw/pbp/[season]/[game]H.html
    """
    return os.path.join(get_season_raw_toi_folder(season), str(game) + 'H.html')


def get_road_shiftlog_filename(season, game):
    """
    Returns the filename of the parsed toi html road shifts
    :param season: int, current season
    :param game: int, game
    :return: /scrape/data/raw/pbp/[season]/[game]H.html
    """
    return os.path.join(get_season_raw_toi_folder(season), str(game) + 'R.html')


def get_player_ids_filename():
    return os.path.join(get_other_data_folder(), 'PLAYER_INFO.feather')
