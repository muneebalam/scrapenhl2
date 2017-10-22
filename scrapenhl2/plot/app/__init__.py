"""
This file imports other views and shows the homepage

/ lists links to homepages for games, help, players, stats, and teams.
"""

from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def index():
    links = {'Click here to navigate games': '/games/',
             'Click here to navigate players': '/players/',
             'Click here to navigate teams': '/teams/',
             'Click here to navigate player stats': '/stats/',
             'Click here to navigate other tasks': '/other/'}
    return render_template('index.html', linklist=links, pagetitle='Home')
