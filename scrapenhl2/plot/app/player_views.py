"""
This file manages pages related to player data.

/players/ delivers a player list with links
/players/[yyyy-yy]/ lists players active in these years, sorted by name, with links, and button to update data
/player/all/[playerid]/ lists chart types for this player, with links to each chart, and dropdown to switch players
/player/all/[playerid]/[charttype]/ goes to a specific chart, with a dropdown to switch chart or player
"""

import io

import pandas as pd
from flask import render_template, send_file, url_for, redirect, request

from scrapenhl2.manipulate import manipulate as manip
from scrapenhl2.plot import app
from scrapenhl2.plot import visualize_player as vp
from scrapenhl2.scrape import scrape_setup as ss


def get_active_player_chart_types():
    return {'rollingcf': vp.rolling_player_cf}


@app.app.route('/players/')
@app.app.route('/players/<seasonrange>/')
def player_index(seasonrange=None):
    pinfo = ss.get_player_ids_file().rename(columns={'ID': 'PlayerID'})

    if seasonrange is not None and seasonrange != 'all':
        if '-' in seasonrange:
            start, end = seasonrange.split('-')
        else:
            start = seasonrange[:4]
            end = seasonrange[4:]
        start = int(start)
        end = int(end)
        end = end - 1
        if end < 2000:
            end += 2000

        players = pd.concat([manip.get_player_5v5_log(season) for season in range(start, end + 1)])
        players = pd.DataFrame({'PlayerID': players.PlayerID.unique()})

        pinfo = pinfo.merge(players, how='inner', on='PlayerID')

    pinfo = pinfo.sort_values(by=['Name', 'DOB'])

    pinfo.loc[:, 'Link'] = pinfo.PlayerID.apply(lambda x: '/players/all/{0:d}/'.format(int(x)))
    pinfo = pinfo[['DOB', 'Hand', 'Name', 'Pos', 'Height', 'Weight', 'Link']]
    return render_template('players.html', pinfo=pinfo)


@app.app.route('/players/all/<int:playerid>/', methods=['GET'])
def player_chart_index(playerid):
    return redirect(url_for('player_chart', playerid=playerid, startseason=ss.get_current_season(),
                            endseason=ss.get_current_season() + 1, charttype='rollingcf'))


def get_player_chart_page_url(playerid, charttype, startseason, endseason):
    return '/players/all/{0:d}/{1:d}-{2:d}/{3:s}/'.format(playerid, startseason, endseason, charttype)


def get_game_chart_fig_url(playerid, charttype, startseason, endseason):
    return '/players/all/{0:d}/{1:d}-{2:d}/{3:s}/fig'.format(playerid, startseason, endseason, charttype)


@app.app.route('/players/all/<int:playerid>/<int:startseason>-<int:endseason>/<charttype>/', methods=['GET', 'POST'])
def player_chart(playerid, charttype, startseason, endseason):
    title = '{0:s} {1:s}'.format(ss.player_as_str(playerid), charttype)
    charttypes = list(get_active_player_chart_types().keys())
    if request.method == 'POST':
        charttype = request.form['charttype']
        startseason = int(request.form['startseason'])
        endseason = int(request.form['endseason'])
    return render_template('individual.html', charttypes=charttypes, pname=ss.player_as_str(playerid),
                           playerid=playerid, startseason=startseason, endseason=endseason, pagetitle=title,
                           charttype=charttype, currentseason=ss.get_current_season())


@app.app.route('/players/all/<int:playerid>/<int:startseason>-<int:endseason>/<charttype>/refresh')
def player_chart_refresh(playerid, charttype, startseason, endseason):
    if endseason == ss.get_current_season() + 1:
        manip.generate_5v5_player_log(endseason - 1)
    return redirect(url_for('player_chart', playerid=playerid, startseason=startseason, endseason=endseason,
                            charttype=charttype))


@app.app.route('/players/all/<int:playerid>/<int:startseason>-<int:endseason>/<charttype>/fig')
def player_chart_background(playerid, charttype, startseason, endseason):
    fig = get_active_player_chart_types()[charttype](playerid, startseason=startseason, endseason=endseason - 1,
                                                     save_file='fig')
    img = io.BytesIO()
    fig.savefig(img)
    img.seek(0)
    return send_file(img, mimetype='image/png')
