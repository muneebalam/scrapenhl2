"""
This file contains the information needed to create the game pages in the app.

The page has a dropdown for the season, dropdown for the game, a radio button to select chart type,
and a button to update.
"""
import pandas as pd
import datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

import scrapenhl2.scrape.schedules as schedules
import scrapenhl2.scrape.players as players
import scrapenhl2.plot.rolling_cf_gf as rolling_cf_gf
import scrapenhl2.plot.rolling_boxcars as rolling_boxcars
import scrapenhl2.plot.visualization_helper as vhelper
import scrapenhl2.scrape.team_info as team_info


def generate_table(dataframe):
    """Transforms a pandas dataframe into an HTML table"""
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([html.Td(dataframe.iloc[i][col]) for col in dataframe.columns]) for i in range(len(dataframe))])


def get_player_graph_types():
    """Update this with more chart types for individuals"""
    options = [{'label': 'Rolling CF%', 'value': 'CF'},
               {'label': 'Rolling GF%', 'value': 'GF'},
               {'label': 'Rolling boxcars', 'value': 'Box'}]
    return options


def get_player_options():
    """Returns list of First Last (DOB)"""
    df = players.get_player_ids_file()[['Name', 'DOB', 'ID']].sort_values('Name')
    names = list(df.Name)
    dobs = list(df.DOB)
    pids = list(df.ID)
    namedob = [{'label': '{0:s} ({1:s})'.format(name, dob), 'value': pid} for name, dob, pid in zip(names, dobs, pids)]
    return namedob


def get_log_for_player(player):
    """Checks player game log and return dataframe of games"""
    if isinstance(player, str):
        name = player[:player.index('(')].strip()
        dob = player[player.index('(') + 1:player.index(')')].strip()
        pid = players.player_as_id(player, dob=dob)
    else:
        pid = player
    df = players.get_player_log_file().query('ID == {0:d}'.format(int(pid)))[['Game', 'Season', 'Team']]
    return df

def get_min_max_game_date_for_player(player):
    """Checks player game log and returns earliest and latest date player is listed"""
    df = get_log_for_player(player)
    # Join to dates
    df = schedules.attach_game_dates_to_dateframe(df) \
        .query('Game >= 20001 & Game <= 30417')
    return df.Date.min(), df.Date.max()

def get_date_options(player):
    df = get_log_for_player(player)
    df = schedules.attach_game_dates_to_dateframe(df) \
        .query('Game >= 20001 & Game <= 30417') \
        .sort_values('Date')
    options = df.Date + ' (' + df.Team.apply(lambda x: team_info.team_as_str(int(x))) + ')'
    options = [{'label': '{0:s} ({1:s})'.format(date,
                                                team_info.team_as_str(int(team))), 'value': date}
               for date, team in zip(df.Date, df.Team)]
    return options

def get_default_start_end_dates(player):
    mindate, maxdate = get_min_max_game_date_for_player(player)

    temp_min = datetime.datetime.strptime(mindate, '%Y-%m-%d')
    temp_max = datetime.datetime.strptime(maxdate, '%Y-%m-%d')
    try_min = temp_max - datetime.timedelta(days=3 * 365)

    if temp_min >= try_min:
        return mindate, maxdate
    return try_min.strftime(format='%Y-%m-%d'), maxdate


app = dash.Dash()

start_player = 'Connor McDavid (1997-01-13)'
date_options = get_date_options(start_player)
start_startdate, start_enddate = get_default_start_end_dates(start_player)


app.layout = html.Div(children=[
    html.Div(children=[html.H1(children='Welcome to the app for scrapenhl2')]),
    html.Div(children=[html.Label('Select player'),
                       dcc.Dropdown(options=get_player_options(),
                                    value=players.player_as_id(start_player[:start_player.index('(')].strip()),
                                    id='player-dropdown'),
                       html.Label('Select start game'),
                       dcc.Dropdown(id='start-dropdown',
                                    options=date_options,
                                    value=start_startdate),
                       html.Label('Select end game'),
                       dcc.Dropdown(id='end-dropdown',
                                    options=date_options,
                                    value=start_enddate)
                       ],
             style={'columnCount': 3}),
    html.Div(children=[html.Div(children=[html.Label('Select graph type'),
                                          dcc.RadioItems(id='player-graph-radio',
                                                         options=get_player_graph_types(),
                                                         value='CF')]),
                       html.Div(children=[html.Label('Select moving average window'),
                                          dcc.Slider(id='roll-len-slider',
                                                     min=5, max=40, step=5, value=25,
                                                     marks={i: '{0:d} gms'.format(i) for i in range(5, 41, 5)})])
                       ],
             style={'columnCount': 2}),
    html.Div(children=[dcc.Graph(id='graph', style={'width': 800})])])

@app.callback(Output('start-dropdown', 'options'), [Input('player-dropdown', 'value')])
def update_start_date_options(player):
    return get_date_options(player)

@app.callback(Output('end-dropdown', 'options'), [Input('player-dropdown', 'value')])
def update_end_date_options(player):
    return get_date_options(player)

@app.callback(Output('start-dropdown', 'value'), [Input('player-dropdown', 'value')])
def update_start_date_value(player):
    return get_default_start_end_dates(player)[0]

@app.callback(Output('end-dropdown', 'value'), [Input('player-dropdown', 'value')])
def update_end_date_value(player):
    return get_default_start_end_dates(player)[1]


@app.callback(Output('graph', 'figure'), [Input('player-dropdown', 'value'),
                                          Input('start-dropdown', 'value'),
                                          Input('end-dropdown', 'value'),
                                          Input('player-graph-radio', 'value'),
                                          Input('roll-len-slider', 'value')])
def player_graph(playerid, startdate, enddate, graphtype, roll_len):
    if graphtype == 'CF':
        return rolling_f_graph_plotly(playerid, startdate, enddate, roll_len, 'C')
    elif graphtype == 'GF':
        return rolling_f_graph_plotly(playerid, startdate, enddate, roll_len, 'G')
    elif graphtype == 'Box':
        return rolling_boxcar_graph_plotly(playerid, startdate, enddate, roll_len)


def rolling_boxcar_graph_plotly(playerid, startdate, enddate, roll_len):
    # TODO this seems broken...
    kwargs = {'player': playerid,
              'roll_len': roll_len,
              'startdate': startdate,
              'enddate': enddate}
    boxcars = vhelper.get_and_filter_5v5_log(**kwargs)

    boxcars = pd.concat([boxcars[['Season', 'Game']], rolling_boxcars.calculate_boxcar_rates(boxcars)], axis=1)

    col_dict = {col[col.index(' ') + 1:col.index('/')]: col for col in boxcars.columns if col[-3:] == '/60'}

    # Set an index
    boxcars.loc[:, 'Game Number'] = 1
    boxcars.loc[:, 'Game Number'] = boxcars['Game Number'].cumsum()
    boxcars.set_index('Game Number', inplace=True)

    goals = go.Scatter(x=boxcars.index, y=boxcars[col_dict['iG']], fill='tozeroy', name='G', mode='none')
    primaries = go.Scatter(x=boxcars.index, y=boxcars[col_dict['iP1']], fill='tonexty', name='A1', mode='none')
    secondaries = go.Scatter(x=boxcars.index, y=boxcars[col_dict['iP']], fill='tonexty', name='A2', mode='none',
                             opacity=0.5)
    gfon = go.Scatter(x=boxcars.index, y=boxcars[col_dict['GFON']], fill='tonexty', name='Other GFON', mode='none',
                      opacity=0.3)

    title = rolling_boxcars._get_rolling_boxcars_title(**kwargs)

    fig = go.Figure(data=[goals, primaries, secondaries, gfon],
                    layout=go.Layout(title=title,
                                     showlegend=True,
                                     legend=go.Legend(x=0, y=1.0),
                                     margin=go.Margin(l=40, r=0, t=40, b=30)))
    return fig

def rolling_f_graph_plotly(playerid, startdate, enddate, roll_len, gfcf):
    kwargs = {'roll_len': roll_len,
              'player': playerid,
              'startdate': startdate,
              'enddate': enddate}

    # Copy paste this code from rolling_f_graph

    fa = vhelper.get_and_filter_5v5_log(**kwargs)

    df = pd.concat([fa[['Season', 'Game']], rolling_cf_gf._calculate_f_rates(fa, gfcf)], axis=1)
    col_dict = {col[col.index(' ') + 1:]: col for col in df.columns if '%' in col}

    df.loc[:, 'Game Number'] = 1
    df.loc[:, 'Game Number'] = df['Game Number'].cumsum()
    df.set_index('Game Number', inplace=True)

    label1 = gfcf + 'F%'
    label2 = gfcf + 'F% Off'
    title = rolling_cf_gf._get_rolling_f_title(gfcf, **kwargs)

    cfon_line = go.Scatter(x=df.index, y=df[col_dict[label1]], mode='lines', name=label1)
    cfoff_line = go.Scatter(x=df.index, y=df[col_dict[label2]], mode='lines', name=label2, line=dict(dash='dash'))

    fig = go.Figure(data=[cfon_line, cfoff_line],
                    layout=go.Layout(title=title,
                                     showlegend=True,
                                     legend=go.Legend(x=0, y=1.0),
                                     margin=go.Margin(l=40, r=0, t=40, b=30)))
                                     #xaxis='Game',
                                     #yaxis=gfcf + 'F%'))
    return fig


def browse_player_charts():
    print('Go to http://127.0.0.1:8050/')
    app.run_server(debug=True)


browse_player_charts()
