"""
This module contains methods for automatically scraping and parsing games.
"""

import os
import os.path
import urllib.error
import urllib.request

import scrapenhl2.scrape.manipulate_schedules as manipulate_schedules
import scrapenhl2.scrape.parse_pbp as parse_pbp
import scrapenhl2.scrape.parse_toi as parse_toi
import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.scrape_pbp as scrape_pbp
import scrapenhl2.scrape.scrape_toi as scrape_toi
import scrapenhl2.scrape.teams as teams


def delete_game_html(season, game):
    """
    Deletes html files. HTML files are used for live game charts, but deleted in favor of JSONs when games go final.

    :param season: int, the season

    :param game: int, the game

    :return: nothing
    """

    for fun in (scrape_pbp.get_game_pbplog_filename,
                scrape_toi.get_home_shiftlog_filename,
                scrape_toi.get_road_shiftlog_filename):
        filename = fun(season, game)
        if os.path.exists(filename):
            os.remove(filename)


def autoupdate(season=None):
    """
    Run this method to update local data. It reads the schedule file for given season and scrapes and parses
    previously unscraped games that have gone final or are in progress. Use this for 2010 or later.

    :param season: int, the season. If None (default), will do current season

    :return: nothing
    """
    # TODO: why does sometimes the schedule have the wrong game-team pairs, but when I regenerate, it's all ok?
    # TODO: this does not work quite right. Doesn't seem to know it needs to re-scrape TOI for previously scraped
    # TODO: in-progress games after they go final

    if season is None:
        season = schedules.get_current_season()

    sch = schedules.get_season_schedule(season)

    # First, for all games that were in progress during last scrape, delete html charts
    inprogress = sch.query('Status == "In Progress"')
    inprogressgames = inprogress.Game.values
    inprogressgames.sort()
    for game in inprogressgames:
        delete_game_html(season, game)

    # Now keep tabs on old final games
    old_final_games = set(sch.query('Status == "Final" & Result != "N/A"').Game.values)

    # Update schedule to get current status
    schedules.generate_season_schedule_file(season)

    # For games done previously, set pbp and toi status to scraped
    manipulate_schedules.update_schedule_with_pbp_scrape(season, old_final_games)
    manipulate_schedules.update_schedule_with_toi_scrape(season, old_final_games)
    sch = schedules.get_season_schedule(season)

    # Now, for games currently in progress, scrape.
    # But no need to force-overwrite. We handled games previously in progress above.
    # Games newly in progress will be written to file here.

    inprogressgames = sch.query('Status == "In Progress"')
    inprogressgames = inprogressgames.Game.values
    inprogressgames.sort()
    print("Updating in-progress games")
    read_inprogress_games(inprogressgames, season)

    # Now, for any games that are final, scrape and parse if not previously done
    games = sch.query('Status == "Final" & Result == "N/A"')
    games = games.Game.values
    games.sort()
    print('Updating final games')
    read_final_games(games, season)

    try:
        teams.update_team_logs(season, force_overwrite=False)
    except Exception as e:
        pass  # ed.print_and_log("Error with team logs in {0:d}: {1:s}".format(season, str(e)), 'warn')


def read_final_games(games, season):
    """

    :param games:
    :param season:

    :return:
    """
    for game in games:
        try:
            scrape_pbp.scrape_game_pbp(season, game, True)
            manipulate_schedules.update_schedule_with_pbp_scrape(season, game)
            parse_pbp.parse_game_pbp(season, game, True)
        except urllib.error.HTTPError as he:
            print('Could not access pbp url for {0:d} {1:d}'.format(season, game))
            print(str(he))
        except urllib.error.URLError as ue:
            print('Could not access pbp url for {0:d} {1:d}'.format(season, game))
            print(str(ue))
        except Exception as e:
            print(str(e))
        try:
            # TODO update only a couple of days later from json and delete html and don't update with toi scrape until then
            if season < 2010:
                scrape_toi.scrape_game_toi_from_html(season, game, True)
                manipulate_schedules.update_schedule_with_toi_scrape(season, game)
                parse_toi.parse_game_toi_from_html(season, game, True)
            else:
                scrape_toi.scrape_game_toi(season, game, True)
                manipulate_schedules.update_schedule_with_toi_scrape(season, game)
                parse_toi.parse_game_toi(season, game, True)

                # If you scrape soon after a game the json only has like the first period for example.
                # If I don't have the full game, use html
                if len(parse_toi.get_parsed_toi(season, game)) < 3600:
                    print('Not enough rows in json for {0:d} {1:d}; reading from html'.format(int(season), int(game)))
                    scrape_toi.scrape_game_toi_from_html(season, game, True)
                    parse_toi.parse_game_toi_from_html(season, game, True)
        except urllib.error.HTTPError as he:
            print('Could not access toi url for {0:d} {1:d}'.format(season, game))
            print(str(he))
        except urllib.error.URLError as ue:
            print('Could not access toi url for {0:d} {1:d}'.format(season, game))
            print(str(ue))
        except Exception as e:
            print(str(e))

        print('Done with {0:d} {1:d} (final)'.format(season, game))


def read_inprogress_games(inprogressgames, season):
    """
    Saves these games to file via html (for toi) and json (for pbp)

    :param inprogressgames: list of int

    :return:
    """

    for game in inprogressgames:
        # scrape_game_pbp_from_html(season, game, False)
        # parse_game_pbp_from_html(season, game, False)
        # PBP JSON updates live, so I can just use that, as before
        scrape_pbp.scrape_game_pbp(season, game, True)
        scrape_toi.scrape_game_toi_from_html(season, game, True)
        parse_pbp.parse_game_pbp(season, game, True)
        parse_toi.parse_game_toi_from_html(season, game, True)
        print('Done with {0:d} {1:d} (in progress)'.format(season, game))


if __name__ == '__main__':
    for season in range(2007, 2010):
        autoupdate(season)
