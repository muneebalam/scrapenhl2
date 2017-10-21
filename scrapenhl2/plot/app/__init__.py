import webbrowser
from io import BytesIO

from flask import Flask, render_template, make_response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

import scrapenhl2

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/update/')
def update_data():
    # TODO add streaming output
    try:
        scrapenhl2.autoupdate()
        return render_template('updatepage.html', msg='Done updating data')
    except Exception as e:
        return render_template('updatepage.html', msg=str(e) + str(e.args))


@app.route('/games/<int:season>/')
@app.route('/games/<int:season>/<team>/')
def get_game_list(season, team=None):
    sch = scrapenhl2.get_season_schedule(season)

    if team is not None:
        tid = scrapenhl2.team_as_id(team)
        sch = sch[(sch.Home == tid) | (sch.Road == tid)]

    sch.loc[:, 'Home'] = sch.Home.apply(lambda x: scrapenhl2.team_as_str(x))
    sch.loc[:, 'Road'] = sch.Road.apply(lambda x: scrapenhl2.team_as_str(x))
    sch = sch[['Date', 'Game', 'Home', 'Road', 'HomeScore', 'RoadScore', 'Status', 'Result']] \
        .rename(columns={'Result': 'HomeResult'})
    sch.loc[:, 'H2H'] = sch.Game.apply(lambda x: '/games/{0:d}/h2h/{1:d}/'.format(season, x))
    sch.loc[:, 'Timeline'] = sch.Game.apply(lambda x: '/games/{0:d}/tl/{1:d}/'.format(season, x))

    return render_template('games.html', season=season, schedule=sch)


@app.route('/games/<int:season>/<charttype>/<int:game>/')
def graph(season, game, charttype):
    # TODO get these working in templates!
    if charttype == 'h2h':
        fig = scrapenhl2.game_h2h(season, game, "fig")
    elif charttype == 'tl':
        fig = scrapenhl2.game_timeline(season, game, "fig")
        pass
    else:
        return render_template("chart.html", message="Invalid chart type; use h2h or tl")
    canvas = FigureCanvas(fig)
    png_output = BytesIO()
    canvas.print_png(png_output)
    response = make_response(png_output.getvalue())
    response.headers['Content-Type'] = 'image/png'

    return response


@app.route('/games/<int:season>/<int:game>/')
def game_graphs(season, game):
    return 'todo'


def runapp(debug=False):
    webbrowser.open('http://127.0.0.1:5000/', new=2)
    app.run()


if __name__ == '__main__':
    runapp(debug=True)
