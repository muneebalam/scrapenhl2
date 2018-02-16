from scrapenhl2.scrape import scrape_pbp, scrape_toi, parse_pbp, parse_toi, autoupdate, teams, players
from scrapenhl2.manipulate import add_onice_players as onice, manipulate as manip
from scrapenhl2.plot import game_timeline, game_h2h, rolling_cf_gf
# autoupdate.autoupdate()

# add_onice_players.add_players_to_file('/Users/muneebalam/Downloads/SJS game.csv',
#                                      focus_team='PHI', time_format='remaining')

# players.update_player_log_file([200, 201, 203, 204, 12], '2017', 'edm_vs_car_2006_game7', 'car', 'P')
rolling_cf_gf.rolling_player_cf('Tom Wilson')
#game_timeline.live_timeline('WSH', 'DET', True, save_file = '/Users/muneebalam/Desktop/tl.png')
#game_h2h.live_h2h('WSH', 'DET', False,  save_file = '/Users/muneebalam/Desktop/h2h.png')

#from scrapenhl2.plot import team_score_shot_rate

#from scrapenhl2.manipulate import manipulate as manip
#manip.get_5v5_player_log(2017, True)

#team_score_shot_rate.team_score_shot_rate_parallel('WSH', 2015, 2016, save_file='/Users/muneebalam/Desktop/score states.png')

# for season in range(2010, 2015):
#    parse_pbp.parse_season_pbp(season, True)
#    teams.update_team_logs(season, True)
