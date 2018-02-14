from twython import Twython
from twython import TwythonStreamer
import re
import time
import os
import datetime
from scrapenhl2.scrape import schedules, games, autoupdate, team_info
from scrapenhl2.plot import game_timeline, game_h2h

if not os.path.exists('bot'):
    os.mkdir('bot')

# Create this file on your own
# See https://opensource.com/article/17/8/raspberry-pi-twitter-bot
from auth import (
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret
)

twitter = Twython(
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret
)

# Only update every 5 mins
LAST_UPDATE = None

# Add team hashtags
HASHTAGS = {'ANA': 'LetsGoDucks', 'ARI': 'Yotes', 'BOS': 'NHLBruins', 'BUF': 'Sabres', 'CGY': 'CofRed',
            'CAR': 'Redvolution', 'CHI': 'Hawks', 'COL': 'GoAvsGo', 'CBJ': 'CBJ', 'DAL': 'GoStars',
            'DET': 'LGRW', 'EDM': 'LetsGoOilers', 'FLA': 'FlaPanthers', 'LAK': 'GoKingsGo', 'MIN': 'mnwild',
            'MTL': 'GoHabsGo', 'NSH': 'Preds', 'NJD': 'NJDevils', 'NYI': 'Isles', 'NYR': 'NYR',
            'OTT': 'Sens', 'PHI': 'LetsGoFlyers', 'PIT': 'Penguins', 'SJS': 'SJSharks', 'TBL': 'GoBolts',
            'STL': 'AllTogetherNowSTL', 'TOR': 'TMLtalk', 'VAN': 'Canucks', 'VGK': 'VegasGoesGold',
            'WSH': 'ALLCAPS', 'WPG': 'GoJetsGo'}

# Message that bot is now active
#twitter.update_status(status="I'm active now ({0:s} ET)".format(
#    datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))


def tweet_error(message, tweetdata):
    """
    Tweets specified message when there's an error

    :param message: str
    :param tweetdata: dict, tweet info
    :return:
    """
    twitter.update_status(status='@{0:s} {1:s}'.format(tweetdata['user']['screen_name'], message),
                          in_reply_to_status_id=tweetdata['id_str'])


def tweet_images(h2hfile, tlfile, hname, rname, status, tweetdata):
    """
    Tweets @ user with H2H and TL charts

    :param h2hfile: filename for h2h chart
    :param tlfile: filename for tl chart
    :param hname: home team name
    :param rname: road team name
    :param status: game status
    :param tweetdata: dict, tweet info
    :return:
    """
    rname = team_info.team_as_str(rname)
    hname = team_info.team_as_str(hname)

    suffix = ''
    if rname in HASHTAGS:
        suffix += ' #' + HASHTAGS[rname]
    if hname in HASHTAGS:
        suffix += ' #' + HASHTAGS[hname]

    with open(h2hfile, 'rb') as photo:
        response = twitter.upload_media(media=photo)
        twitter.update_status(
            status='@{3:s} H2H: {0:s} @ {1:s} ({2:s}){4:s}'.format(rname, hname, status,
                                                                   tweetdata['user']['screen_name'], suffix),
            media_ids=[response['media_id']],
            in_reply_to_status_id = tweetdata['id_str'])
    with open(tlfile, 'rb') as photo:
        response = twitter.upload_media(media=photo)
        twitter.update_status(
            status='@{3:s} TL: {0:s} @ {1:s} ({2:s}){4:s}'.format(rname, hname, status,
                                                                  tweetdata['user']['screen_name'],
                                                                  suffix),
            media_ids=[response['media_id']],
            in_reply_to_status_id = tweetdata['id_str'])

# Gets info about the game from the tweet, updates data if necessary, and posts chart
class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
            print(data['text'])
            global LAST_UPDATE
            try:
                try:
                    season, gameid = games.find_playoff_game(data['text'])
                except ValueError:
                    season = None
                    gameid = None

                # Get season with a 4-digit regex
                if season is None:
                    text = data['text'] + ' '
                    if re.search(r'\s\d{4}\s', text) is not None:
                        season = int(re.search(r'\s\d{4}\s', text).group(0))
                        if season < 2015 or season > schedules.get_current_season():
                            tweet_error("Sorry, I don't have data for this season yet", data)
                            return
                    else:
                        season = schedules.get_current_season()

                # Get game with a 5-digit regex
                if gameid is None:
                    if re.search(r'\s\d{5}\s', text) is not None:
                        gameid = int(re.search(r'\s\d{5}\s', text).group(0))
                        if not schedules.check_valid_game(season, gameid):
                            tweet_error("Sorry, this game ID doesn't look right", data)
                            return
                    else:
                        pass

                if gameid is None:
                    # Get team names
                    parts = data['text'].split(' ')
                    teams = []
                    for part in parts:
                        if re.match(r'[A-z]{3}', part.strip()):
                            part = part.upper()
                            if team_info.team_as_id(part) is not None:
                                teams.append(part)
                    if len(teams) == 0:
                        # Assume this was just a tagging, e.g. follow this account, or thread discussion
                        return
                    elif len(teams) != 2:
                        tweet_error("Sorry, I need 2 teams. Found {0:d}. Make sure abbreviations are correct"
                                    .format(len(teams)), data)
                        return

                    team1, team2 = teams[:2]
                    gameid = games.most_recent_game_id(team1, team2)

                h2hfile = 'bot/{0:d}0{1:d}h2h.png'.format(season, gameid)
                tlfile = 'bot/{0:d}0{1:d}tl.png'.format(season, gameid)

                oldstatus = schedules.get_game_status(season, gameid)

                # If game is in current season
                # If game is today or in the past, and game listed as "scheduled," update schedule
                # If game is in progress and it's been at least 5 min since last date update, then update
                # TODO
                if season == schedules.get_current_season():
                    today = datetime.datetime.now().strftime('%Y-%m-%d')
                    gdata = schedules.get_game_data_from_schedule(season, gameid)
                    if gdata['Date'] == today:
                        if gdata['Status'] == 'Scheduled':
                            autoupdate.autoupdate(season, update_team_logs=True)
                            LAST_UPDATE = time.time()
                        elif gdata['Status'] != 'Final' and \
                                (LAST_UPDATE is None or time.time() - LAST_UPDATE >= 60 * 5):
                            autoupdate.autoupdate(season, update_team_logs=True)
                            LAST_UPDATE = time.time()
                    elif gdata['Date'] < today and gdata['Status'] != 'Final':
                        autoupdate.autoupdate(season, update_team_logs=True)
                        LAST_UPDATE = time.time()


                hname = schedules.get_home_team(season, gameid)
                rname = schedules.get_road_team(season, gameid)
                status = schedules.get_game_status(season, gameid)

                executed = True
                if 'In Progress' in oldstatus or not os.path.exists(tlfile):
                    try:
                        game_timeline.game_timeline(season, gameid, save_file=tlfile)
                    except Exception as e:
                        print(data['text'], time.time(), e, e.args)
                        executed = False

                if 'In Progress' in oldstatus or not os.path.exists(h2hfile):
                    try:
                        game_h2h.game_h2h(season, gameid, save_file=h2hfile)
                    except Exception as e:
                        print(data['text'], time.time(), e, e.args)
                        executed = False

                if executed:
                    tweet_images(h2hfile, tlfile, hname, rname, status, data)
                    print('Success!')
                else:
                    tweet_error("Sorry, there was an unknown error while making the charts (cc @muneebalamcu)", data)
                    return
            except Exception as e:
                print('Unexpected error')
                print(time.time(), data['text'], e, e.args)

# Use this try-catch to post an outgoing message
# I'm using Pycharm, so pressing stop will create a KeyboardInterrupt
try:
    stream = MyStreamer(
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret
    )
    stream.statuses.filter(track='@h2hbot')
except KeyboardInterrupt:
    #twitter.update_status(status="I'm turning off now ({0:s} ET)".format(
    #    datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))
    pass


