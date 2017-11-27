"""
The purpose of this module is to check game data for integrity (e.g. TOI has at least 3600 rows).
"""

from scrapenhl2.scrape import parse_toi, parse_pbp, autoupdate, schedules, teams

def check_game_pbp(season=None):
    """
    Rescrapes gone-final games if they do not pass the following checks:
        - (TODO)

    :param season: int, the season

    :return:
    """


def check_game_toi(season=None):
    """
    Rescrapes gone-final games if they do not pass the following checks:
        - (TODO)

    :param season: int, the season

    :return:
    """
    if season is None:
        season = schedules.get_current_season()

    sch = schedules.get_season_schedule(season)
    finals = sch.query('Status == "Final" & TOIStatus == "Scraped" & Game >= 20001 & Game <= 30417').Game.values

    games_to_rescrape = []

    for game in finals:
        try:
            toi = parse_toi.get_parsed_toi(season, game)

            assert len(toi) >= 3595  # At least 3600 seconds in game, approx

            # TODO add other checks

        except AssertionError as ae:
            print(ae, ae.args, len(toi))

            games_to_rescrape.append(game)
        except IOError:
            games_to_rescrape.append(game)

    if len(games_to_rescrape) > 0:
        autoupdate.read_final_games(games_to_rescrape, season)
        teams.update_team_logs(season, force_games=games_to_rescrape)


def check_team_toi(season=None):
    """

    :param season:
    :return:
    """
