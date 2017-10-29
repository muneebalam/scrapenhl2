"""
This module contains method related to team logs.
"""

import os.path

import feather
import pandas as pd
import pyarrow

import scrapenhl2.scrape.organization as organization
import scrapenhl2.scrape.parse_pbp as parse_pbp
import scrapenhl2.scrape.parse_toi as parse_toi
import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.team_info as team_info


def get_team_pbp(season, team):
    """
    Returns the pbp of given team in given season across all games.

    :param season: int, the season
    :param team: int or str, the team abbreviation.

    :return: df, the pbp of given team in given season
    """
    return feather.read_dataframe(get_team_pbp_filename(season, team_info.team_as_str(team, True)))


def get_team_toi(season, team):
    """
    Returns the toi of given team in given season across all games.

    :param season: int, the season
    :param team: int or str, the team abbreviation.

    :return: df, the toi of given team in given season
    """
    return feather.read_dataframe(get_team_toi_filename(season, team_info.team_as_str(team, True)))


def write_team_pbp(pbp, season, team):
    """
    Writes the given pbp dataframe to file.

    :param pbp: df, the pbp of given team in given season
    :param season: int, the season
    :param team: int or str, the team abbreviation.

    :return: nothing
    """
    if pbp is None:
        print('PBP df is None, will not write team log')
        return
    feather.write_dataframe(pbp, get_team_pbp_filename(season, team_info.team_as_str(team, True)))


def write_team_toi(toi, season, team):
    """
    Writes team TOI log to file

    :param toi: df, team toi for this season
    :param season: int, the season
    :param team: int or str, the team abbreviation.

    :return:
    """
    if toi is None:
        print('TOI df is None, will not write team log')
        return
    try:
        feather.write_dataframe(toi, get_team_toi_filename(season, team_info.team_as_str(team, True)))
    except ValueError:
        # Need dtypes to be numbers or strings. Sometimes get objs instead
        for col in toi:
            try:
                toi.loc[:, col] = pd.to_numeric(toi[col])
            except ValueError:
                toi.loc[:, col] = toi[col].astype(str)
        feather.write_dataframe(toi, get_team_toi_filename(season, team_info.team_as_str(team, True)))


def get_team_pbp_filename(season, team):
    """
    Returns filename of the PBP log for this team and season

    :param season: int, the season
    :param team: int or str, the team abbreviation.

    :return:
    """
    return os.path.join(organization.get_season_team_pbp_folder(season),
                        "{0:s}.feather".format(team_info.team_as_str(team, abbreviation=True)))


def get_team_toi_filename(season, team):
    """
    Returns filename of the TOI log for this team and season

    :param season: int, the season
    :param team: int or str, the team abbreviation.

    :return:
    """
    return os.path.join(organization.get_season_team_toi_folder(season),
                        "{0:s}.feather".format(team_info.team_as_str(team, abbreviation=True)))


def update_team_logs(season, force_overwrite=False):
    """
    This method looks at the schedule for the given season and writes pbp for scraped games to file.
    It also adds the strength at each pbp event to the log.

    :param season: int, the season
    :param force_overwrite: bool, whether to generate from scratch

    :return: nothing
    """

    # For each team

    new_games_to_do = schedules.get_season_schedule(season).query('Status == "Final"')
    new_games_to_do = new_games_to_do[(new_games_to_do.Game >= 20001) & (new_games_to_do.Game <= 30417)]
    allteams = sorted(list(new_games_to_do.Home.append(new_games_to_do.Road).unique()))

    for teami, team in enumerate(allteams):
        print('Updating team log for {0:d} {1:s}'.format(season, team_info.team_as_str(team)))

        # Compare existing log to schedule to find missing games
        newgames = new_games_to_do[(new_games_to_do.Home == team) | (new_games_to_do.Road == team)]
        if force_overwrite:
            pbpdf = None
            toidf = None
        else:
            # Read currently existing ones for each team and anti join to schedule to find missing games
            try:
                pbpdf = get_team_pbp(season, team)
                newgames = newgames.merge(pbpdf[['Game']].drop_duplicates(), how='outer', on='Game', indicator=True)
                newgames = newgames[newgames._merge == "left_only"].drop('_merge', axis=1)
            except FileNotFoundError:
                pbpdf = None
            except pyarrow.lib.ArrowIOError:  # pyarrow (feather) FileNotFoundError equivalent
                pbpdf = None

            try:
                toidf = get_team_toi(season, team)
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
                gamepbp = parse_pbp.get_parsed_pbp(season, game)
                gametoi = parse_toi.get_parsed_toi(season, game)
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

        write_team_pbp(pbpdf, season, team)
        write_team_toi(toidf, season, team)
        print('Done with team logs for {0:d} {1:s} ({2:d}/{3:d})'.format(
            season, team_info.team_as_str(team), teami + 1, len(allteams)))


def team_setup():
    """
    Creates team log-related folders.

    :return: nothing
    """
    for season in range(2005, schedules.get_current_season() + 1):
        organization.check_create_folder(organization.get_season_team_pbp_folder(season))
    for season in range(2005, schedules.get_current_season() + 1):
        organization.check_create_folder(organization.get_season_team_toi_folder(season))


team_setup()
