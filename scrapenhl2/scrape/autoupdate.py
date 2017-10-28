"""
This module contains methods for automatically scraping and parsing games.
"""


def update_team_logs(season, force_overwrite=False):
    """
    This method looks at the schedule for the given season and writes pbp for scraped games to file.
    It also adds the strength at each pbp event to the log.
    :param season: int, the season
    :param force_overwrite: bool, whether to generate from scratch
    :return:
    """

    # For each team


    new_games_to_do = get_files.get_season_schedule(season).query('Status == "Final"')
    new_games_to_do = new_games_to_do[(new_games_to_do.Game >= 20001) & (new_games_to_do.Game <= 30417)]
    allteams = sorted(list(new_games_to_do.Home.append(new_games_to_do.Road).unique()))

    for teami, team in enumerate(allteams):
        spinner.start(text='Updating team log for {0:d} {1:s}\n'.format(season, ss.team_as_str(team)))

        # Compare existing log to schedule to find missing games
        newgames = new_games_to_do[(new_games_to_do.Home == team) | (new_games_to_do.Road == team)]
        if force_overwrite:
            pbpdf = None
            toidf = None
        else:
            # Read currently existing ones for each team and anti join to schedule to find missing games
            try:
                pbpdf = ss.get_team_pbp(season, team)
                newgames = newgames.merge(pbpdf[['Game']].drop_duplicates(), how='outer', on='Game', indicator=True)
                newgames = newgames[newgames._merge == "left_only"].drop('_merge', axis=1)
            except FileNotFoundError:
                pbpdf = None
            except pyarrow.lib.ArrowIOError:  # pyarrow (feather) FileNotFoundError equivalent
                pbpdf = None

            try:
                toidf = ss.get_team_toi(season, team)
            except FileNotFoundError:
                toidf = None
            except pyarrow.lib.ArrowIOError:  # pyarrow (feather) FileNotFoundError equivalent
                toidf = None

        for i, gamerow in newgames.iterrows():
            game = gamerow[1]
            home = gamerow[2]
            road = gamerow[4]

            # load parsed pbp and toi
            try:
                gamepbp = get_parsed_pbp(season, game)
                gametoi = get_parsed_toi(season, game)
                # TODO 2016 20779 why does pbp have 0 rows?
                # Also check for other errors in parsing etc

                if len(gamepbp) > 0 and len(gametoi) > 0:
                    # Rename score and strength columns from home/road to team/opp
                    if team == home:
                        gametoi = gametoi.assign(TeamStrength=gametoi.HomeStrength, OppStrength=gametoi.RoadStrength) \
                            .drop({'HomeStrength', 'RoadStrength'}, axis=1)
                        gamepbp = gamepbp.assign(TeamScore=gamepbp.HomeScore, OppScore=gamepbp.RoadScore) \
                            .drop({'HomeScore', 'RoadScore'}, axis=1)
                    else:
                        gametoi = gametoi.assign(TeamStrength=gametoi.RoadStrength, OppStrength=gametoi.HomeStrength) \
                            .drop({'HomeStrength', 'RoadStrength'}, axis=1)
                        gamepbp = gamepbp.assign(TeamScore=gamepbp.RoadScore, OppScore=gamepbp.HomeScore) \
                            .drop({'HomeScore', 'RoadScore'}, axis=1)

                    # add scores to toi and strengths to pbp
                    gamepbp = gamepbp.merge(gametoi[['Time', 'TeamStrength', 'OppStrength']], how='left', on='Time')
                    gametoi = gametoi.merge(gamepbp[['Time', 'TeamScore', 'OppScore']], how='left', on='Time')
                    gametoi.loc[:, 'TeamScore'] = gametoi.TeamScore.fillna(method='ffill')
                    gametoi.loc[:, 'OppScore'] = gametoi.OppScore.fillna(method='ffill')

                    # Switch TOI column labeling from H1/R1 to Team1/Opp1 as appropriate
                    cols_to_change = list(gametoi.columns)
                    cols_to_change = [x for x in cols_to_change if len(x) == 2]  # e.g. H1
                    if team == home:
                        swapping_dict = {'H': 'Team', 'R': 'Opp'}
                        colchanges = {c: swapping_dict[c[0]] + c[1] for c in cols_to_change}
                    else:
                        swapping_dict = {'H': 'Opp', 'R': 'Team'}
                        colchanges = {c: swapping_dict[c[0]] + c[1] for c in cols_to_change}
                    gametoi = gametoi.rename(columns=colchanges)

                    # finally, add game, home, and road to both dfs
                    gamepbp.loc[:, 'Game'] = game
                    gamepbp.loc[:, 'Home'] = home
                    gamepbp.loc[:, 'Road'] = road
                    gametoi.loc[:, 'Game'] = game
                    gametoi.loc[:, 'Home'] = home
                    gametoi.loc[:, 'Road'] = road

                    # concat toi and pbp
                    if pbpdf is None:
                        pbpdf = gamepbp
                    else:
                        pbpdf = pd.concat([pbpdf, gamepbp])
                    if toidf is None:
                        toidf = gametoi
                    else:
                        toidf = pd.concat([toidf, gametoi])

            except FileNotFoundError:
                pass

        # write to file
        if pbpdf is not None:
            pbpdf.loc[:, 'FocusTeam'] = team
        if toidf is not None:
            toidf.loc[:, 'FocusTeam'] = team

        ss.write_team_pbp(pbpdf, season, team)
        ss.write_team_toi(toidf, season, team)
        # ed.print_and_log('Done with team logs for {0:d} {1:s} ({2:d}/{3:d})'.format(
        #    season, ss.team_as_str(team), teami + 1, len(allteams)), print_and_log=False)
        spinner.stop()
        # ed.print_and_log('Updated team logs for {0:d}'.format(season))


def update_player_logs_from_page(pbp, season, game):
    """
    Takes the game play by play and adds players to the master player log file, noting that they were on the roster
    for this game, which team they played for, and their status (P for played, S for scratch).
    :param season: int, the season
    :param game: int, the game
    :param pbp: json, the pbp of the game
    :return: nothing
    """

    # Get players who played, and scratches, from boxscore
    home_played = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'players')
    road_played = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'players')
    home_scratches = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'home', 'scratches')
    road_scratches = ss.try_to_access_dict(pbp, 'liveData', 'boxscore', 'teams', 'away', 'scratches')

    # Played are both dicts, so make them lists
    home_played = [int(pid[2:]) for pid in home_played]
    road_played = [int(pid[2:]) for pid in road_played]

    # Played may include scratches, so make sure to remove them
    home_played = list(set(home_played).difference(set(home_scratches)))
    road_played = list(set(road_played).difference(set(road_scratches)))

    # Get home and road names
    gameinfo = ss.get_game_data_from_schedule(season, game)

    # Update player logs
    ss.update_player_log_file(home_played, season, game, gameinfo['Home'], 'P')
    ss.update_player_log_file(home_scratches, season, game, gameinfo['Home'], 'S')
    ss.update_player_log_file(road_played, season, game, gameinfo['Road'], 'P')
    ss.update_player_log_file(road_scratches, season, game, gameinfo['Road'], 'S')

    # TODO: One issue is we do not see goalies (and maybe skaters) who dressed but did not play. How can this be fixed?














def autoupdate(season=None):
    """
    Run this method to update local data. It reads the schedule file for given season and scrapes and parses
    previously unscraped games that have gone final or are in progress.
    :param season: int, the season. If None (default), will do current season
    :return: nothing
    """

    if season is None:
        season = get_files.get_current_season()

    if season < 2010:
        autoupdate_old(season)
    else:
        autoupdate_new(season)


def autoupdate_old(season):
    """
    Run this method to update local data. It reads the schedule file for given season and scrapes and parses
    previously unscraped games that have gone final or are in progress. Use this for 2007, 2008, or 2009.
    :param season: int, the season. If None (default), will do current season
    :return: nothing
    """
    # TODO


def autoupdate_new(season):
    """
    Run this method to update local data. It reads the schedule file for given season and scrapes and parses
    previously unscraped games that have gone final or are in progress. Use this for 2010 or later.
    :param season: int, the season. If None (default), will do current season
    :return: nothing
    """
    # TODO: why does sometimes the schedule have the wrong game-team pairs, but when I regenerate, it's all ok?

    sch = get_files.get_season_schedule(season)

    spinner = halo.Halo()

    # First, for all games that were in progress during last scrape, delete html charts
    spinner.start(text="Deleting data from previously in-progress games")
    inprogress = sch.query('Status == "In Progress"')
    inprogressgames = inprogress.Game.values
    inprogressgames.sort()
    for game in inprogressgames:
        ss.delete_game_html(season, game)

    # Now keep tabs on old final games
    old_final_games = set(sch.query('Status == "Final"').Game.values)

    # Update schedule to get current status
    ss.generate_season_schedule_file(season)
    ss.refresh_schedules()
    sch = get_files.get_season_schedule(season)

    # For games done previously, set pbp and toi status to scraped
    _ = ss.update_schedule_with_pbp_scrape(season, old_final_games)
    sch = ss.update_schedule_with_toi_scrape(season, old_final_games)

    # Now, for games currently in progress, scrape.
    # But no need to force-overwrite. We handled games previously in progress above.
    # Games newly in progress will be written to file here.
    spinner.stop()

    inprogressgames = sch.query('Status == "In Progress"')
    inprogressgames = inprogressgames.Game.values
    inprogressgames.sort()
    spinner.start(text="Updating in-progress games")
    read_inprogress_games(inprogressgames, season)
    spinner.stop()

    # Now, for any games that are final, scrape and parse if not previously done
    games = sch.query('Status == "Final" & PBPStatus == "N/A"')
    games = games.Game.values
    games.sort()
    spinner.start(text='Updating final games')
    read_final_games(games, season)
    spinner.stop()
    ss.refresh_schedules()

    try:
        update_team_logs(season, force_overwrite=False)
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
            scrape_game_pbp(season, game, True)
            _ = ss.update_schedule_with_pbp_scrape(season, game)
            parse_game_pbp(season, game, True)
        except urllib.error.HTTPError as he:
            ed.print_and_log('Could not access pbp url for {0:d} {1:d}'.format(season, game), 'warn')
            ed.print_and_log(str(he), 'warn')
        except urllib.error.URLError as ue:
            ed.print_and_log('Could not access pbp url for {0:d} {1:d}'.format(season, game), 'warn')
            ed.print_and_log(str(ue), 'warn')
        except Exception as e:
            ed.print_and_log(str(e), 'warn')
        try:
            scrape_game_toi(season, game, True)
            _ = ss.update_schedule_with_toi_scrape(season, game)
            parse_game_toi(season, game, True)
        except urllib.error.HTTPError as he:
            ed.print_and_log('Could not access toi url for {0:d} {1:d}'.format(season, game), 'warn')
            ed.print_and_log(str(he), 'warn')
        except urllib.error.URLError as ue:
            ed.print_and_log('Could not access toi url for {0:d} {1:d}'.format(season, game), 'warn')
            ed.print_and_log(str(ue), 'warn')
        except Exception as e:
            ed.print_and_log(str(e), 'warn')

        ed.print_and_log('Done with {0:d} {1:d} (final)'.format(season, game), print_and_log=False)


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
        scrape_game_pbp(season, game, True)
        scrape_game_toi_from_html(season, game, True)
        parse_game_pbp(season, game, True)
        parse_game_toi_from_html(season, game, True)
        ed.print_and_log('Done with {0:d} {1:d} (in progress)'.format(season, game))


if __name__ == '__main__':
    autoupdate()
