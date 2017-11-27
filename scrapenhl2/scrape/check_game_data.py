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

    games_rescraped = None
    sch = schedules.get_season_schedule(season)
    finals = sch.query('Status == "Final" & TOIStatus == "Scraped"').Game.values

    for game in finals:
        try:
            toi = parse_toi.get_parsed_toi(season, game)

            assert len(toi) >= 3600  # At least 3600 seconds in game

            # TODO add other checks
            
        except AssertionError as ae:
            print(ae, ae.args)
            autoupdate.read_final_games([game], season)
            if games_rescraped is None:
                games_rescraped = []
            games_rescraped.append(game)

    if games_rescraped is not None:
        teams.update_team_logs(season, force_games=games_rescraped)