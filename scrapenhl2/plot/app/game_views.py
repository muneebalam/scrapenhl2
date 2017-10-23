"""
This file manages pages for looking at and updating game data.

/games/ delivers a season list with links and says date of last update (most recent scrape)
/games/[yyyy]/ lists games in this year, sorted by ID, with links
/games/[yyyy]/[gameid]/ lists links for charts in this game, and button to update data, and dropdown to switch games.
/games/[yyyy]/[gameid]/[charttype]/ shows this chart, with a dropdown to switch chart or game
"""

import io
from functools import update_wrapper, wraps

from flask import render_template, send_file, url_for, redirect, make_response

from scrapenhl2.plot import app
from scrapenhl2.plot import visualize_game as vg
from scrapenhl2.scrape import scrape_game as sg
from scrapenhl2.scrape import scrape_setup as ss


def nocache(f):
    @wraps(f)
    def new_func(*args, **kwargs):
        resp = make_response(f(*args, **kwargs))
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0'
        return resp

    return update_wrapper(new_func, f)


def get_active_game_chart_types():
    return {'h2h': vg.game_h2h, 'timeline': vg.game_timeline}


@app.app.route('/games/')
def game_index():
    links = {'Click here for {0:d}-{1:s}'.format(season, str(season + 1)[2:]): '/games/{0:d}/'.format(season) for
             season in range(2010, 2018)}
    links['Click here to go back'] = '/'
    return render_template('index.html', linklist=links, pagetitle='Games')


@app.app.route('/games/<int:season>/')
def game_season_index(season):
    if season == ss.get_current_season():
        ss.generate_season_schedule_file(season)
        ss.refresh_schedules()
    sch = ss.get_season_schedule(season)

    sch.loc[:, 'Home'] = sch.Home.apply(lambda x: ss.team_as_str(x))
    sch.loc[:, 'Road'] = sch.Road.apply(lambda x: ss.team_as_str(x))
    sch = sch[['Date', 'Game', 'Home', 'Road', 'HomeScore', 'RoadScore', 'Status', 'Result']] \
        .rename(columns={'Result': 'HomeResult'})
    sch.loc[:, 'Charts'] = sch.Game.apply(lambda x: get_game_url(season, x))

    return render_template('games.html', season=season, schedule=sch,
                           pagetitle='Games in {0:d}-{1:s}'.format(season, str(season + 1)[2:]))


def get_game_url(season, game):
    return '/games/{0:d}/{1:d}/'.format(season, game)


@app.app.route('/games/<int:season>/<int:game>/')
def game_game_index(season, game):
    gameinfo = ss.get_game_data_from_schedule(season, game)
    hname = ss.team_as_str(gameinfo['Home'])
    rname = ss.team_as_str(gameinfo['Road'])
    links = {charttype: get_game_chart_page_url(season, game, charttype) for
             charttype in get_active_game_chart_types()}
    return render_template('index.html', linklist=links,
                           pagetitle='{0:d}-{1:s} {2:d}\n{3:s} at {4:s}\n{5:s}'.format(
                               season, str(season + 1)[2:], game, rname, hname, gameinfo['Date']))


def get_game_chart_page_url(season, game, charttype):
    return '/games/{0:d}/{1:d}/{2:s}/'.format(season, game, charttype)


def get_game_chart_fig_url(season, game, charttype):
    return '/games/{0:d}/{1:d}/{2:s}/fig'.format(season, game, charttype)


@app.app.route('/games/<int:season>/<int:game>/<charttype>/')
@nocache
def game_chart(season, game, charttype):
    title = '{0:d}-{1:s} {2:d} {3:s}'.format(season, str(season + 1)[2:], game, charttype)
    status = ss.get_game_status(season, game)
    return render_template('game_chart.html', pagetitle=title, season=season, game=game, charttype=charttype,
                           show_refresh=status not in {'Scheduled', 'Final'})


@app.app.route('/games/<int:season>/<int:game>/<charttype>/refresh')
@nocache
def game_chart_refresh(season, game, charttype):
    sg.read_inprogress_games([game], season)
    return redirect(url_for('game_chart', season=season, game=game, charttype=charttype))


@app.app.route('/games/<int:season>/<int:game>/<charttype>/fig')
@nocache
def game_chart_background(season, game, charttype):
    fig = get_active_game_chart_types()[charttype](season, game, 'fig')
    img = io.BytesIO()
    fig.savefig(img)
    img.seek(0)
    return send_file(img, mimetype='image/png')
