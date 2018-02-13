# ! /usr/bin/env python3
# -*- coding: utf-8 -*-

from scrapenhl2 import scrape
from scrapenhl2.manipulate.manipulate import (
    time_to_mss,
    _filter_for_scores
)
from unittest.mock import call, MagicMock
from pytest_mock import mocker

import pandas as pd

def test_get_pbp_events():
    pass

def test_filter_for_event_types():
    pass

def test_filter_for_scores(mocker):
    mydf = pd.DataFrame({'TeamScore': [0, 0, 0, 1, 1, 1, 2, 2, 2],
                         'OppScore': [0, 1, 2, 0, 1, 2, 0, 1, 2],
                         'Row': list(range(1, 10))})
    # No value
    assert _filter_for_scores(mydf, noscorekwarg=0).equals(mydf) is True
    # Single value
    assert _filter_for_scores(mydf, score_diff=0).equals(mydf.iloc[[0, 3, 6], :]) is True
    # Multiple values, including negative
    assert _filter_for_scores(mydf, score_diff=[-1, 1]) \
               .sort_values('Row').equals(mydf.iloc[[2, 4, 6, 8], :]) is True

def test_filter_for_strengths():
    pass

def test_filter_for_times():
    pass

def test_filter_for_games():
    pass

def test_filter_for_players():
    pass

def test_join_on_ice_players_to_pbp():
    pass

def test_filter_for_team():
    pass

def test_seasons_to_read():
    pass

def test_teams_to_read():
    pass

def test_get_5v5_player_game_toi():
    pass

def test_generate_player_toion_toioff():
    pass

def test_count_by_keys():
    pass

def test_get_5v5_player_game_boxcars():
    pass

def test_get_5v5_player_game_toicomp():
    pass

def test_long_on_player_and_opp():
    pass

def test_merge_toi60_position_calculate_sums():
    pass

def test_retrieve_start_end_times():
    pass

def test_get_5v5_player_game_shift_startend():
    pass

def test_get_directions_for_xy_for_season():
    pass

def test_get_directions_for_xy_for_game():
    pass

def test_infer_zones_for_faceoffs():
    pass

def test_generate_5v5_player_log():
    pass

def test_get_5v5_player_game_fa():
    pass

def test_merge_onto_all_team_games_and_zero_fill():
    pass

def test_convert_to_all_combos():
    pass

def test_get_player_toi():
    pass

def test_get_line_combos():
    pass

def test_get_pairings():
    pass

def test_get_game_h2h_toi():
    pass

def test_filter_for_event_types2():
    pass

def test_get_game_h2h_corsi():
    pass

def test_time_to_mss(mocker):
    # 0:00
    assert time_to_mss(0) == '0:00'
    # 10:01
    assert time_to_mss(601) == "10:01"
    # 10:10
    assert time_to_mss(610) == "10:10"

def test_team_5v5_score_state_summary_by_game():
    pass

def test_team_5v5_shot_rates_by_score():
    pass

def test_add_score_adjustment_to_team_pbp():
    pass
