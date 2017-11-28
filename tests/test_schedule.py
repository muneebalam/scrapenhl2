#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from scrapenhl2.scrape.schedules import (
    _get_current_season,
    schedule_setup,
    get_season_schedule_filename,
    get_season_schedule,
    get_team_schedule,
    write_season_schedule,
    get_game_data_from_schedule,
    _CURRENT_SEASON,
    _SCHEDULES,
)
from unittest.mock import call, MagicMock

def test_get_current_season(mocker):

    now_mock = mocker.patch("arrow.now")

    date_mock = now_mock.return_value
    date_mock.year = 2017
    date_mock.month = 8

    assert _get_current_season() == 2016
    date_mock.month = 9
    assert _get_current_season() == 2017
    date_mock.month = 10
    assert _get_current_season() == 2017


def test_get_season_schedule_filename(mocker):

    organization_mock = mocker.patch(
        "scrapenhl2.scrape.schedules.organization"
    )
    organization_mock.get_other_data_folder.return_value = "/tmp"

    assert get_season_schedule_filename(2017) == "/tmp/2017_schedule.feather"


def test_schedule_setup(mocker):

    current_season_mock = mocker.patch(
        "scrapenhl2.scrape.schedules._get_current_season"
    )
    current_season_mock.return_value = 2006
    get_season_schedule_mock = mocker.patch(
        "scrapenhl2.scrape.schedules.get_season_schedule_filename"
    )
    get_season_schedule_mock.side_effect = ["tmp/2005", "tmp/2006"]
    path_exists_mock = mocker.patch(
        "os.path.exists"
    )
    path_exists_mock.side_effect = [True, False]
    gen_schedule_file_mock = mocker.patch(
        "scrapenhl2.scrape.schedules.generate_season_schedule_file"
    )
    season_schedule_mock = mocker.patch(
        "scrapenhl2.scrape.schedules._get_season_schedule"
    )

    schedule_setup()
    get_season_schedule_mock.assert_has_calls([call(2005), call(2006)])
    path_exists_mock.assert_has_calls(get_season_schedule_mock.return_value)
    gen_schedule_file_mock.assert_has_calls([call(2006)])
    season_schedule_mock.assert_has_calls([call(2005), call(2006)])


def test_write_season_schedule(mocker):

    feather_mock = mocker.patch("scrapenhl2.scrape.schedules.feather")
    dataframe_mock = MagicMock()

    ret = write_season_schedule(dataframe_mock, 2017, True)

    feather_mock.write_dataframe.assert_called_once_with(
        dataframe_mock, get_season_schedule_filename(2017)
    )

    get_season_schedule_mock = mocker.patch(
        "scrapenhl2.scrape.schedules.get_season_schedule"
    )
    panda_mock = mocker.patch("scrapenhl2.scrape.schedules.pd")

    ret = write_season_schedule(dataframe_mock, 2017, False)

    assert get_season_schedule_mock().query.called_once_with("Status != Final")
    assert panda_mock.concat.called_once_with(
        get_season_schedule_mock().query()
    )

def test_get_game_data_from_schedule(mocker):

    get_season_schedule_mock = mocker.patch(
        "scrapenhl2.scrape.schedules.get_season_schedule"
    )

    get_game_data_from_schedule(2017, 1234)
    get_season_schedule_mock.assert_called_once_with(2017)
    get_season_schedule_mock().query.assert_called_once_with('Game == 1234')
    get_season_schedule_mock().query().to_dict.assert_called_once_with(orient='series')




