from twython import Twython
from twython import TwythonStreamer
import re
import time
import os
import datetime
from scrapenhl2.scrape import schedules, games, autoupdate, team_info, teams
from scrapenhl2.plot import game_timeline, game_h2h, rolling_cf_gf

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

SILENT = True

SCRAPED_NEW = False

# Add team hashtags
HASHTAGS = {'ANA': 'LetsGoDucks', 'ARI': 'Yotes', 'BOS': 'NHLBruins', 'BUF': 'Sabres', 'CGY': 'CofRed',
            'CAR': 'Redvolution', 'CHI': 'Hawks', 'COL': 'GoAvsGo', 'CBJ': 'CBJ', 'DAL': 'GoStars',
            'DET': 'LGRW', 'EDM': 'LetsGoOilers', 'FLA': 'FlaPanthers', 'LAK': 'GoKingsGo', 'MIN': 'mnwild',
            'MTL': 'GoHabsGo', 'NSH': 'Preds', 'NJD': 'NJDevils', 'NYI': 'Isles', 'NYR': 'NYR',
            'OTT': 'Sens', 'PHI': 'LetsGoFlyers', 'PIT': 'Penguins', 'SJS': 'SJSharks', 'TBL': 'GoBolts',
            'STL': 'AllTogetherNowSTL', 'TOR': 'TMLtalk', 'VAN': 'Canucks', 'VGK': 'VegasGoesGold',
            'WSH': 'ALLCAPS', 'WPG': 'GoJetsGo'}

# Message that bot is now active
if not SILENT:
    twitter.update_status(status="I'm active now ({0:s} ET)".format(
        datetime.datetime.now().strftime('%Y-%m-%d %-H:%M ET')))


def tweet_error(message, tweetdata):
    """
    Tweets specified message when there's an error

    :param message: str
    :param tweetdata: dict, tweet info
    :return:
    """
    twitter.update_status(status='@{0:s} {1:s}'.format(tweetdata['user']['screen_name'], message),
                          in_reply_to_status_id=tweetdata['id_str'])


def tweet_player_graphs(cffile, gffile, pname, tweetdata):
    """

    :param file:
    :param name:
    :param tweetdata:
    :return:
    """
    with open(cffile, 'rb') as cfphoto, open(gffile, 'rb') as gfphoto:
        cfresponse = twitter.upload_media(media=cfphoto)
        gfresponse = twitter.upload_media(media=gfphoto)
        twitter.update_status(status='@{0:s} {1:s} rolling CF% and GF%'.format(tweetdata['user']['screen_name'],
                                                                               pname),
                              media_ids=[cfresponse['media_id'], gfresponse['media_id']],
                              in_reply_to_status_id=tweetdata['id_str'])


def tweet_game_images(h2hfile, tlfile, hname, rname, status, tweetdata):
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

    with open(h2hfile, 'rb') as h2hphoto, open(tlfile, 'rb') as tlphoto:
        h2hresponse = twitter.upload_media(media=h2hphoto)
        tlresponse = twitter.upload_media(media=tlphoto)

        twitter.update_status(
            status='@{3:s} H2H and TL: {0:s} @ {1:s} ({2:s}){4:s}'.format(rname, hname, status,
                                                                          tweetdata['user']['screen_name'], suffix),
            media_ids=[h2hresponse['media_id'], tlresponse['media_id']],
            in_reply_to_status_id = tweetdata['id_str'])


def find_seasons_in_text(tweettext):
    """
    Searches for regex pattern \d{4}
    :param text: str
    :return: list of int
    """
    text = tweettext + ' '
    return [int(x) for x in re.findall(r'\s\d{4}\s', text)]


def check_player_cf_graph_tweet_format(text):
    """
    Checks if tweet has cf or cf% in it, corsi also ok
    :param text: str
    :return: bool
    """
    return re.search(r'\scf%?\s', text.lower() + ' ') is not None or \
           re.search(r'\scorsi%?\s', text.lower() + ' ') is not None


def player_cf_graphs(tweetdata):
    """
    If tweet fits
    :param tweetdata:
    :return:
    """
    if not check_player_cf_graph_tweet_format(tweetdata['text']):
        return False

    pname = (tweetdata['text'] + ' ') \
        .replace(' cf ', '') \
        .replace('@h2hbot ', '') \
        .replace(' dates ', '') \
        .strip()
    fname = 'bot/' + pname.replace(' ', '_') + '_cf.png'
    fname2 = 'bot/' + pname.replace(' ', '_') + '_gf.png'

    kwargs = {}
    for i, season in enumerate(find_seasons_in_text(tweetdata['text'])):
        if i == 0:
            kwargs['startseason'] = season
        else:
            kwargs['endseason'] = season

    try:
        rolling_cf_gf.rolling_player_cf(tweetdata['text'], save_file=fname, **kwargs)
        rolling_cf_gf.rolling_player_gf(tweetdata['text'], save_file=fname2, **kwargs)
        tweet_player_graphs(fname, fname2, pname, tweetdata)
        print('Success!')
    except Exception as e:
        tweet_error("Sorry, there was an unknown error while making the charts (cc @muneebalamcu). "
                    "Might have had issues identifying the player", tweetdata)

    return True


class MyStreamer(TwythonStreamer):
    """
    Gets info about the game from the tweet, updates data if necessary, and posts chart
    """
    def on_success(self, data):
        if 'text' in data:
            print(data['text'])

            if r'https://t.co/' in data['text']:
                print('This looks like an image')
                return
            if data['text'][:3] == 'RT ':
                print('This looks like a retweet')
                return

            global LAST_UPDATE, SCRAPED_NEW
            try:
                if player_cf_graphs(data):
                    return

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
                            print('Invalid season')
                            return
                    else:
                        season = schedules.get_current_season()

                # Get game with a 5-digit regex
                if gameid is None:
                    if re.search(r'\s\d{5}\s', text) is not None:
                        gameid = int(re.search(r'\s\d{5}\s', text).group(0))
                        if not schedules.check_valid_game(season, gameid):
                            tweet_error("Sorry, this game ID doesn't look right", data)
                            print('Game ID not right')
                            return
                    else:
                        pass

                if gameid is None:
                    # Get team names
                    parts = data['text'].replace('@h2hbot', '').strip().split(' ')
                    teams = []
                    for part in parts:
                        if re.match(r'[A-z]{3}', part.strip()):
                            part = part.upper()
                            if team_info.team_as_id(part) is not None:
                                teams.append(part)
                    if len(teams) == 0:
                        print('Think this was a tagged discussion')
                        return
                    elif len(teams) != 2:
                        tweet_error("Sorry, I need 2 teams. Found {0:d}. Make sure abbreviations are correct"
                                    .format(len(teams)), data)
                        return

                    team1, team2 = teams[:2]
                    gameid = games.most_recent_game_id(team1, team2)

                oldstatus = schedules.get_game_status(season, gameid)

                # Scrape only if:
                # Game is in current season AND
                # Game is today, and my schedule says it's "scheduled", OR
                # Game is today, and my schedule doesn't say it's final yet, and it's been at least
                #   5 min since last scrape, OR
                # Game was before today and my schedule doesn't say "final"
                # Update in these cases
                scrapeagain = False
                if season == schedules.get_current_season():
                    today = datetime.datetime.now().strftime('%Y-%m-%d')
                    gdata = schedules.get_game_data_from_schedule(season, gameid)
                    if gdata['Date'] == today:
                        if gdata['Status'] == 'Scheduled':
                            scrapeagain = True
                        elif gdata['Status'] != 'Final' and \
                                (LAST_UPDATE is None or time.time() - LAST_UPDATE >= 60 * 5):
                            scrapeagain = True
                    elif gdata['Date'] < today and gdata['Status'] != 'Final':
                        scrapeagain = True
                if scrapeagain:
                    autoupdate.autoupdate(season, update_team_logs=False)
                    LAST_UPDATE = time.time()
                    SCRAPED_NEW = True

                hname = schedules.get_home_team(season, gameid)
                rname = schedules.get_road_team(season, gameid)
                status = schedules.get_game_status(season, gameid)

                h2hfile = 'bot/{0:d}0{1:d}h2h_{2:s}.png'.format(season, gameid, status[:5].lower())
                tlfile = 'bot/{0:d}0{1:d}tl_{2:s}.png'.format(season, gameid, status[:5].lower())

                try:
                    if not (status == 'Final' and os.path.exists(tlfile)):
                        game_timeline.game_timeline(season, gameid, save_file=tlfile)
                    if not (status == 'Fina' and os.path.exists(h2hfile)):
                        game_h2h.game_h2h(season, gameid, save_file=h2hfile)
                    tweet_game_images(h2hfile, tlfile, hname, rname, status, data)
                    print('Success!')
                except Exception as e:
                    print(data['text'], time.time(), e, e.args)
                    tweet_error("Sorry, there was an unknown error while making the charts (cc @muneebalamcu)",
                                data)

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
    if not SILENT:
        twitter.update_status(status="I'm turning off now ({0:s})".format(
            datetime.datetime.now().strftime('%Y-%m-%d %-H:%M ET')))
    if SCRAPED_NEW:
        teams.update_team_logs(schedules.get_current_season())


