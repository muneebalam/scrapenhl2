"""
This file contains the information needed to create the game pages in the app.

The page has a dropdown for the season, dropdown for the game, a radio button to select chart type,
and a button to update.
"""
import os

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import flask

import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.team_info as team_info
import scrapenhl2.scrape.organization as organization
import scrapenhl2.plot.game_h2h as game_h2h
import scrapenhl2.plot.game_timeline as game_timeline

def get_images_url():
    """Returns /static/"""
    return '/static/'


def get_game_images_url():
    """Returns /static/game/"""
    return '{0:s}{1:s}'.format(get_images_url(), 'game/')


def get_game_image_url(season, game, charttype):
    """Returns /static/game/2017/20001/H2H.png for example"""
    return '{0:s}{1:d}/{2:d}/{3:s}.png'.format(get_game_images_url(), season, game, charttype)


def get_player_images_url():
    """Returns /static/player/"""
    return '{0:s}{1:s}'.format(get_images_url(), 'player/')


def get_team_images_url():
    """Returns /static/team/"""
    return '{0:s}{1:s}'.format(get_images_url(), 'team/')


def get_images_folder():
    """Returns scrapenhl2/plot/app/_static/"""
    return os.path.join(organization.get_base_dir(), 'plot', 'app', '_static')


def clean_images_folder():
    """Removes all files in scrapenhl2/plot/app/_static/"""
    filelist = [os.path.join(get_images_folder(), file) for file in os.listdir(get_images_folder())]
    for file in filelist:
        os.unlink(file)


def get_game_image_filename(season, game, charttype):
    """Returns e.g. scrapenhl2/plot/app/_static/2017-20001-H2H.png"""
    return os.path.join(get_images_folder(), '{0:d}-{1:d}-{2:s}.png'.format(season, game, charttype))


def generate_table(dataframe):
    """Transforms a pandas dataframe into an HTML table"""
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([html.Td(dataframe.iloc[i][col]) for col in dataframe.columns]) for i in range(len(dataframe))])


def reduced_schedule_dataframe(season):
    """Returns schedule[Date, Game, Road, Home, Status]"""
    sch = schedules.get_season_schedule(season).drop({'Season', 'PBPStatus', 'TOIStatus'}, axis=1)
    sch.loc[:, 'Home'] = sch.Home.apply(lambda x: team_info.team_as_str(x))
    sch.loc[:, 'Road'] = sch.Road.apply(lambda x: team_info.team_as_str(x))
    sch = sch[['Date', 'Game', 'Road', 'Home', 'Status']].query('Game >= 20001 & Game <= 30417')
    return sch

def get_season_dropdown_options():
    """Use for options in season dropdown"""
    options = [{'label': '{0:d}-{1:s}'.format(yr, str(yr + 1)[2:]),
                'value': yr} for yr in range(2010, schedules.get_current_season()+1)]
    return options

def get_game_dropdown_options_for_season(season):
    """Use for options in game dropdown"""
    sch = reduced_schedule_dataframe(season)
    options = [{'label': '{0:s}: {1:d} {2:s}@{3:s} ({4:s})'.format(date, game, road, home, status),
                'value': game} for index, date, game, road, home, status in sch.itertuples()]
    return options

def get_game_graph_types():
    """Update this with more chart types for single games"""
    options = [{'label': 'Head-to-head', 'value': 'H2H'},
               {'label': 'Game timeline', 'value': 'TL'}]
    return options

#sch = reduced_schedule_dataframe(schedules.get_current_season())
clean_images_folder()

app = dash.Dash()

app.layout = html.Div(children=[html.H1(children='Welcome to the app for scrapenhl2'),
                                html.Label('Select season'),
                                dcc.Dropdown(
                                    options=get_season_dropdown_options(),
                                    value=schedules.get_current_season(),
                                    id='season-dropdown'),
                                html.Label('Select game'),
                                dcc.Dropdown(
                                    options=get_game_dropdown_options_for_season(schedules.get_current_season()),
                                    value=20001,
                                    id='game-dropdown'),
                                html.Label('Select graph type'),
                                dcc.RadioItems(
                                    id='game-graph-radio',
                                    options=get_game_graph_types(),
                                    value='H2H'),
                                html.Img(id='image', width=800)
                                ])

@app.callback(Output('game-dropdown', 'options'), [Input('season-dropdown', 'value')])
def update_game_dropdown_options_for_season(selected_season):
    return get_game_dropdown_options_for_season(selected_season)

@app.callback(Output('image', 'src'), [Input('season-dropdown', 'value'),
                                       Input('game-dropdown', 'value'),
                                       Input('game-graph-radio', 'value')])
def update_game_graph(selected_season, selected_game, selected_chart):
    if selected_chart == 'H2H':
        game_h2h.game_h2h(selected_season, selected_game,
                          save_file=get_game_image_filename(selected_season, selected_game, selected_chart))
    elif selected_chart == 'TL':
        game_timeline.game_timeline(selected_season, selected_game,
                                    save_file=get_game_image_filename(selected_season, selected_game, selected_chart))
    return get_game_image_url(selected_season, selected_game, selected_chart)


@app.server.route('{0:s}<season>/<game>/<charttype>.png'.format(get_game_images_url()))
def serve_game_image(season, game, charttype):
    fname = get_game_image_filename(int(season), int(game), charttype)
    fname = fname[fname.rfind('/') + 1:]
    return flask.send_from_directory(get_images_folder(), fname)


def browse_game_charts():
    print('Go to http://127.0.0.1:8050/')
    app.run_server(debug=True)
