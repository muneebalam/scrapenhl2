"""
This module contains several helpful methods for accessing files.
At import, this module creates folders for data storage if need be.

It also creates a team ID mapping and schedule files from 2005 through the current season (if the files do not exist).
"""

import os
import os.path
import os.path


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

    # ------- Team logs -------
    for season in range(2005, get_files.get_current_season() + 1):
        get_filenames.check_create_folder(get_filenames.get_season_team_pbp_folder(season))
    for season in range(2005, get_files.get_current_season() + 1):
        get_filenames.check_create_folder(get_filenames.get_season_team_toi_folder(season))

    # ------- Other stuff -------


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






