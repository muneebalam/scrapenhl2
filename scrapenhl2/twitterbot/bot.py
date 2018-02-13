from twython import Twython
from twython import TwythonStreamer
import re
import time
import os.path
import datetime
from scrapenhl2.scrape import schedules, games, autoupdate, team_info
from scrapenhl2.plot import game_timeline, game_h2h


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

LAST_UPDATE = None

twitter.update_status(status="I'm active now ({0:s} ET)".format(
    datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))

def tweet_images(h2hfile, tlfile, hname, rname, status, tweetdata):
    """

    :param h2hfile: filename for h2h chart
    :param tlfile: filename for tl chart
    :param hname: home team name
    :param rname: road team name
    :param status: game status
    :param user: user to tweet at
    :return:
    """
    rname = team_info.team_as_str(rname)
    hname = team_info.team_as_str(hname)

    with open(h2hfile, 'rb') as photo:
        response = twitter.upload_media(media=photo)
        twitter.update_status(status='@{3:s} H2H: {0:s} @ {1:s} ({2:s})'.format(rname, hname, status,
                                                                                tweetdata['user']['screen_name']),
                              media_ids=[response['media_id']],
                              in_reply_to_status_id = tweetdata['id_str'])
    with open(tlfile, 'rb') as photo:
        response = twitter.upload_media(media=photo)
        twitter.update_status(status='@{3:s} TL: {0:s} @ {1:s} ({2:s})'.format(rname, hname, status,
                                                                               tweetdata['user']['screen_name']),
                              media_ids=[response['media_id']],
                              in_reply_to_status_id = tweetdata['id_str'])

class MyStreamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
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
                            return
                    else:
                        season = schedules.get_current_season()

                # Get game with a 5-digit regex
                if gameid is None:
                    if re.search(r'\s\d{5}\s', text) is not None:
                        gameid = int(re.search(r'\s\d{5}\s', text).group(0))
                        if not schedules.check_valid_game(season, gameid):
                            return
                    else:
                        pass

                if gameid is None:
                    # Get team names
                    parts = data['text'].split(' ')
                    teams = []
                    for part in parts:
                        if re.match(r'[A-z]{3}', part.strip()):
                            teams.append(part.upper())
                    if len(teams) != 2:
                        return

                    team1, team2 = teams[:2]
                    gameid = games.most_recent_game_id(team1, team2)

                hname = schedules.get_home_team(season, gameid)
                rname = schedules.get_road_team(season, gameid)
                status = schedules.get_game_status(season, gameid)

                h2hfile = '/Users/muneebalam/Desktop/bot/{0:d}0{1:d}h2h.png'.format(season, gameid)
                tlfile = '/Users/muneebalam/Desktop/bot/{0:d}0{1:d}tl.png'.format(season, gameid)

                # Only update if game is in progress and has been at least five minutes since last update
                if 'In Progress' in status and (LAST_UPDATE is None or time.time() - LAST_UPDATE >= 60 * 5):
                    autoupdate.autoupdate(season)
                    LAST_UPDATE = time.time()

                executed = True
                if 'In Progress' in status or not os.path.exists(tlfile):
                    try:
                        game_timeline.game_timeline(season, gameid, save_file=tlfile)
                    except Exception as e:
                        print(data['text'], time.time(), e, e.args)
                        executed = False

                if 'In Progress' in status or not os.path.exists(h2hfile):
                    try:
                        game_h2h.game_h2h(season, gameid, save_file=h2hfile)
                    except Exception as e:
                        print(data['text'], time.time(), e, e.args)
                        executed = False

                if executed:
                    tweet_images(h2hfile, tlfile, hname, rname, status, data)
            except Exception as e:
                print('Unexpected error')
                print(time.time(), data['text'], e, e.args)

try:
    stream = MyStreamer(
        consumer_key,
        consumer_secret,
        access_token,
        access_token_secret
    )
    stream.statuses.filter(track='@h2hbot')
except KeyboardInterrupt:
    twitter.update_status(status="I'm turning off ({0:s} ET)".format(
        datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))


