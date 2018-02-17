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

    assert schedules.get_team_schedule().equals(pd.Series([2, 3, 4]))

def test_clear_caches(mocker):
    """Trivial, will not test"""

