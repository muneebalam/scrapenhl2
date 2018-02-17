#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from scrapenhl2.scrape import schedules

def test__get_current_season(mocker):
    """Method detects season based on today's date"""

    now_mock = mocker.patch("arrow.now")
    date_mock = now_mock.return_value
    date_mock.year = 2017
    date_mock.month = 8

    assert schedules._get_current_season() == 2016
    date_mock.month = 9
    assert schedules._get_current_season() == 2017
    date_mock.month = 10

def test_get_current_season(mocker):
    """Trivial, will not test"""

def test_get_schedule_filename(mocker):
    """Method returns filename of schedule SQL"""
    organization_mock = mocker.patch("scrapenhl2.scrape.schedules.organization")
    organization_mock.get_other_data_folder.return_value = "/tmp"
    assert schedules.get_schedule_filename() == "/tmp/schedule.sqlite"

def test_get_schedule_connection(mocker):
    """Trivial, will not test"""

def test_close_schedule_cursor(mocker):
    """Trivial, will not test"""

def test_get_season_schedule(mocker):
    """Not tested"""

def test_get_schedule(mocker):
    """Not tested"""

def test_get_schedule_table_colnames_coltypes(mocker):
    """Trivial, will not test"""

def test__create_schedule_table(mocker):
    """Not tested"""

def test_write_schedules(mocker):
    """Not tested"""

def test_get_team_schedule(mocker):
    """Not tested"""

def test_get_team_games(mocker):
    """Method gets team games subject to args"""

    team_sch_mock = mocker.patch('schedules.get_team_schedule')
    team_sch_mock.return_value = pd.DataFrame({'Col1': [1, 2, 3], 'Game': [2, 3, 4]})

    assert schedules.get_team_schedule(2017, 'WSH').equals(pd.Series([2, 3, 4]))

def test_clear_caches(mocker):
    """Trivial, will not test"""

def test_get_game_data_from_schedule(mocker):
    """Method gets values for game in dict form"""

    sql_mock = mocker.patch('pd.read_sql_query')
    sql_mock.return_value = pd.DataFrame({'A': [0], 'B': [1], 'C': [2]})
    assert schedules.get_game_data_from_schedule(2017, 20001) == {'A': 0, 'B': 1, 'C': 2}

def test_get_game_date(mocker):
    """Trivial, will not test"""

def test_get_home_team(mocker):
    """Trivial, will not test"""

def test_get_road_team(season, game, returntype='id'):
    """Trivial, will not test"""

def test_get_home_score(season, game):
    """Trivial, will not test"""

def test_get_road_score(season, game):
    """Trivial, will not test"""

def test_get_game_status(season, game):
    """Trivial, will not test"""

def test_get_game_result(season, game):
    """Trivial, will not test"""

def test_get_season_schedule_url(season):
    """Trivial, will not test"""

def test_get_teams_in_season(season):
    """Trivial, will not test"""

def test_check_valid_game(season, game):
    """Trivial, will not test"""

def test_schedule_setup():
    """Trivial, will not test"""

def test_generate_season_schedule_file(season):
    """Not tested"""

def test__add_schedule_from_json(season, jsondict):
    """Not tested"""

def test_attach_game_dates_to_dateframe(df):
    """Not tested"""

