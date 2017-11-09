"""
This module contains methods for scraping TOI.
"""

import json
import os.path
import urllib.request
import zlib
from time import sleep

from scrapenhl2.scrape import organization
from scrapenhl2.scrape import schedules
from scrapenhl2.scrape import general_helpers as helpers


def scrape_game_toi(season, game, force_overwrite=False):
    """
    This method scrapes the toi for the given game.

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If file exists already, won't scrape again

    :return: nothing
    """
    filename = get_game_raw_toi_filename(season, game)
    if not force_overwrite and os.path.exists(filename):
        return False

    page = helpers.try_url_n_times(get_shift_url(season, game))
    save_raw_toi(page, season, game)
    # ed.print_and_log('Scraped toi for {0:d} {1:d}'.format(season, game))
    sleep(1)  # Don't want to overload NHL servers

    # It's most efficient to parse with page in memory, but for sake of simplicity will do it later
    # toi = read_toi_from_page(page)
    return True


def get_home_shiftlog_filename(season, game):
    """
    Returns the filename of the parsed toi html home shifts

    :param season: int, the season
    :param game: int, the game

    :return: str, /scrape/data/raw/pbp/[season]/[game]H.html
    """
    return os.path.join(organization.get_season_raw_toi_folder(season), str(game) + 'H.html')


def get_road_shiftlog_filename(season, game):
    """
    Returns the filename of the parsed toi html road shifts

    :param season: int, current season
    :param game: int, game
    :return: str, /scrape/data/raw/pbp/[season]/[game]H.html
    """
    return os.path.join(organization.get_season_raw_toi_folder(season), str(game) + 'R.html')


def scrape_game_toi_from_html(season, game, force_overwrite=True):
    """
    This method scrapes the toi html logs for the given game.

    :param season: int, the season
    :param game: int, the game
    :param force_overwrite: bool. If file exists already, won't scrape again

    :return: nothing
    """
    filenames = (get_home_shiftlog_filename(season, game), get_road_shiftlog_filename(season, game))
    urls = (get_home_shiftlog_url(season, game), get_road_shiftlog_url(season, game))
    filetypes = ('H', 'R')
    for i in range(2):
        filename = filenames[i]
        if not force_overwrite and os.path.exists(filename):
            pass

        page = helpers.try_url_n_times(urls[i])
        save_raw_toi_from_html(page, season, game, filetypes[i])
        sleep(1)  # Don't want to overload NHL servers
        print('Scraped html toi for {0:d} {1:d}'.format(season, game))


def save_raw_toi(page, season, game):
    """
    Takes the bytes page containing shift information and saves to disk as a compressed zlib.

    :param page: bytes. str(page) would yield a string version of the json shifts
    :param season: int, the season
    :param game: int, the game

    :return: nothing
    """
    page2 = zlib.compress(page, level=9)
    filename = get_game_raw_toi_filename(season, game)
    w = open(filename, 'wb')
    w.write(page2)
    w.close()


def save_raw_toi_from_html(page, season, game, homeroad):
    """
    Takes the bytes page containing shift information and saves to disk as html.

    :param page: bytes. str(page) would yield a string version of the json shifts
    :param season: int, he season
    :param game: int, the game
    :param homeroad: str, 'H' or 'R'

    :return: nothing
    """
    if homeroad == 'H':
        filename = get_home_shiftlog_filename(season, game)
    elif homeroad == 'R':
        filename = get_road_shiftlog_filename(season, game)
    w = open(filename, 'w')
    w.write(page.decode('latin-1'))
    w.close()


def get_raw_html_toi(season, game, homeroad):
    """
    Loads the html file containing this game's toi from disk.

    :param season: int, the season
    :param game: int, the game
    :param homeroad: str, 'H' for home or 'R' for road

    :return: str, the html toi
    """
    if homeroad == 'H':
        filename = get_home_shiftlog_filename(season, game)
    elif homeroad == 'R':
        filename = get_road_shiftlog_filename(season, game)
    with open(filename, 'r') as reader:
        page = reader.read()
    return page


def get_raw_toi(season, game):
    """
    Loads the compressed json file containing this game's shifts from disk.

    :param season: int, the season
    :param game: int, the game

    :return: dict, the json shifts
    """
    with open(get_game_raw_toi_filename(season, game), 'rb') as reader:
        page = reader.read()
    return json.loads(str(zlib.decompress(page).decode('latin-1')))


def get_home_shiftlog_url(season, game):
    """
    Gets the url for a page containing shift information for specified game from HTML tables for home team.

    :param season: int, the season
    :param game: int, the game

    :return : str, e.g. http://www.nhl.com/scores/htmlreports/20072008/TH020001.HTM
    """
    return 'http://www.nhl.com/scores/htmlreports/{0:d}{1:d}/TH0{2:d}.HTM'.format(season, season + 1, game)


def get_road_shiftlog_url(season, game):
    """
    Gets the url for a page containing shift information for specified game from HTML tables for road team.

    :param season: int, the season
    :param game: int, the game

    :return : str, e.g. http://www.nhl.com/scores/htmlreports/20072008/TV020001.HTM
    """
    return 'http://www.nhl.com/scores/htmlreports/{0:d}{1:d}/TV0{2:d}.HTM'.format(season, season + 1, game)


def get_shift_url(season, game):
    """
    Gets the url for a page containing shift information for specified game from NHL API.

    :param season: int, the season
    :param game: int, the game

    :return : str, http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId=[season]0[game]
    """
    return 'http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId={0:d}0{1:d}'.format(season, game)


def get_game_raw_toi_filename(season, game):
    """
    Returns the filename of the raw toi folder

    :param season: int, current season
    :param game: int, game

    :return: str, /scrape/data/raw/toi/[season]/[game].zlib
    """
    return os.path.join(organization.get_season_raw_toi_folder(season), str(game) + '.zlib')


def scrape_toi_setup():
    """
    Creates raw toi folders if need be

    :return:
    """
    for season in range(2005, schedules.get_current_season() + 1):
        organization.check_create_folder(organization.get_season_raw_toi_folder(season))


scrape_toi_setup()
