"""
This module contains methods for scraping pbp.
"""

import json
import os.path
import urllib.request
import zlib
from time import sleep

import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.general_helpers as helpers


def scrape_game_pbp_from_html(season, game, force_overwrite=True):
    """
    This method scrapes the html pbp for the given game. Use for live games.

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If file exists already, won't scrape again

    :return: bool, False if not scraped, else True
    """
    filename = get_game_pbplog_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    page = get_game_from_url(season, game)
    save_raw_html_pbp(page, season, game)
    # ed.print_and_log('Scraped html pbp for {0:d} {1:d}'.format(season, game))
    sleep(1)  # Don't want to overload NHL servers

    # It's most efficient to parse with page in memory, but for sake of simplicity will do it later
    # pbp = read_pbp_events_from_page(page)
    # update_team_logs(pbp, season, schedule_item['Home'])
    return True


def scrape_game_pbp(season, game, force_overwrite=False):
    """
    This method scrapes the pbp for the given game.

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If file exists already, won't scrape again

    :return: bool, False if not scraped, else True
    """
    filename = get_game_raw_pbp_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    # Use the season schedule file to get the home and road team names
    # schedule_item = get_files.get_season_schedule(season) \
    #    .query('Game == {0:d}'.format(game)) \
    #    .to_dict(orient = 'series')
    # The output format of above was {colname: np.array[vals]}. Change to {colname: val}
    # schedule_item = {k: v.values[0] for k, v in schedule_item.items()}

    page = get_game_from_url(season, game)
    save_raw_pbp(page, season, game)
    # ed.print_and_log('Scraped pbp for {0:d} {1:d}'.format(season, game))
    sleep(1)  # Don't want to overload NHL servers

    # It's most efficient to parse with page in memory, but for sake of simplicity will do it later
    # pbp = read_pbp_events_from_page(page)
    # update_team_logs(pbp, season, schedule_item['Home'])
    return True


def save_raw_html_pbp(page, season, game):
    """
    Takes the bytes page containing html pbp information and saves as such

    :param page: bytes
    :param season: int, the season
    :param game: int, the game

    :return: nothing
    """
    filename = get_game_pbplog_filename(season, game)
    w = open(filename, 'w')
    w.write(page.decode('latin-1'))
    w.close()


def save_raw_pbp(page, season, game):
    """
    Takes the bytes page containing pbp information and saves to disk as a compressed zlib.

    :param page: bytes. str(page) would yield a string version of the json pbp
    :param season: int, the season
    :param game: int, the game

    :return: nothing
    """
    page2 = zlib.compress(page, level=9)
    filename = get_game_raw_pbp_filename(season, game)
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
    with open(get_game_raw_pbp_filename(season, game), 'rb') as reader:
        page = reader.read()
    return json.loads(str(zlib.decompress(page).decode('latin-1')))


def get_raw_html_pbp(season, game):
    """
    Loads the html file containing this game's play by play from disk.

    :param season: int, the season
    :param game: int, the game

    :return: str, the html pbp
    """
    with open(get_game_pbplog_filename(season, game), 'r') as reader:
        page = reader.read()
    return page


def get_game_from_url(season, game):
    """
    Gets the page containing information for specified game from NHL API.

    :param season: int, the season
    :param game: int, the game

    :return: str, the page at the url
    """

    return helpers.try_url_n_times(get_game_url(season, game))


def get_game_pbplog_url(season, game):
    """
    Gets the url for a page containing pbp information for specified game from HTML tables.

    :param season: int, the season
    :param game: int, the game

    :return : str, e.g. http://www.nhl.com/scores/htmlreports/20072008/PL020001.HTM
    """
    return 'http://www.nhl.com/scores/htmlreports/{0:d}{1:d}/PL0{2:d}.HTM'.format(season, season + 1, game)


def get_game_url(season, game):
    """
    Gets the url for a page containing information for specified game from NHL API.

    :param season: int, the season
    :param game: int, the game

    :return: str, https://statsapi.web.nhl.com/api/v1/game/[season]0[game]/feed/live
    """
    return 'https://statsapi.web.nhl.com/api/v1/game/{0:d}0{1:d}/feed/live'.format(season, game)


def get_game_raw_pbp_filename(season, game):
    """
    Returns the filename of the raw pbp folder

    :param season: int, current season
    :param game: int, game

    :return: str, /scrape/data/raw/pbp/[season]/[game].zlib
    """
    return os.path.join(organization.get_season_raw_pbp_folder(season), str(game) + '.zlib')


def get_game_pbplog_filename(season, game):
    """
    Returns the filename of the parsed pbp html game pbp

    :param season: int, current season
    :param game: int, game

    :return: str, /scrape/data/raw/pbp/[season]/[game].html
    """
    return os.path.join(organization.get_season_raw_pbp_folder(season), str(game) + '.html')


def scrape_pbp_setup():
    """
    Creates raw pbp folders if need be

    :return:
    """
    for season in range(2005, schedules.get_current_season() + 1):
        organization.check_create_folder(organization.get_season_raw_pbp_folder(season))


scrape_pbp_setup()
