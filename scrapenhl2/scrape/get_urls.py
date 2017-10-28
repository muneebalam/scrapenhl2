"""
This module contains all methods finding URLs.
"""




def get_game_url(season, game):
    """
    Gets the url for a page containing information for specified game from NHL API.
    :param season: int
        the season
    :param game: int
        the game
    :return: str
        https://statsapi.web.nhl.com/api/v1/game/[season]0[game]/feed/live
    """
    return 'https://statsapi.web.nhl.com/api/v1/game/{0:d}0{1:d}/feed/live'.format(season, game)


def get_game_pbplog_url(season, game):
    """
    Gets the url for a page containing pbp information for specified game from HTML tables.
    :param season: int
        the season
    :param game: int
        the game
    :return : str
        e.g. http://www.nhl.com/scores/htmlreports/20072008/PL020001.HTM
    """
    return 'http://www.nhl.com/scores/htmlreports/{0:d}{1:d}/PL0{2:d}.HTM'.format(season, season + 1, game)


def get_home_shiftlog_url(season, game):
    """
    Gets the url for a page containing shift information for specified game from HTML tables for home team.
    :param season: int
        the season
    :param game: int
        the game
    :return : str
        e.g. http://www.nhl.com/scores/htmlreports/20072008/TH020001.HTM
    """
    return 'http://www.nhl.com/scores/htmlreports/{0:d}{1:d}/TH0{2:d}.HTM'.format(season, season + 1, game)


def get_road_shiftlog_url(season, game):
    """
    Gets the url for a page containing shift information for specified game from HTML tables for road team.
    :param season: int
        the season
    :param game: int
        the game
    :return : str
        e.g. http://www.nhl.com/scores/htmlreports/20072008/TV020001.HTM
    """
    return 'http://www.nhl.com/scores/htmlreports/{0:d}{1:d}/TV0{2:d}.HTM'.format(season, season + 1, game)


def get_shift_url(season, game):
    """
    Gets the url for a page containing shift information for specified game from NHL API.
    :param season: int
        the season
    :param game: int
        the game
    :return : str
        http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId=[season]0[game]
    """
    return 'http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId={0:d}0{1:d}'.format(season, game)


def get_player_url(playerid):
    """
    Gets the url for a page containing information for specified player from NHL API.
    :param playerid: int
        the player ID
    :return: str
        https://statsapi.web.nhl.com/api/v1/people/[playerid]
    """
    return 'https://statsapi.web.nhl.com/api/v1/people/{0:s}'.format(str(playerid))


