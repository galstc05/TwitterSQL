#!/usr/bin/env python2


from TwitterSearch import *
from ConfigParser import SafeConfigParser
from datetime import datetime
import sqlite3
import os
import httplib
import sys
import re
from urlparse import urlparse
reload(sys)
sys.setdefaultencoding('ASCII')


#path to config file
config_file = 'user.cfg'
#path to database
db_name = 'data.db'

_consumer_key = ''
_consumer_secret = ''
_access_token = ''
_access_token_secret = ''


#
# Initialze the script. Setup Database, load config file.
#
def init():
    
    initDB()
    
    parser = SafeConfigParser()
    parser.read(config_file)
    
    global _consumer_key, _consumer_secret, _access_token, _access_token_secret
    
    _consumer_key = parser.get('config', 'consumer_key')
    _consumer_secret = parser.get('config', 'consumer_secret')
    _access_token = parser.get('config', 'access_token')
    _access_token_secret = parser.get('config', 'access_token_secret')


#
# Create database
#
def initDB():
    if os.path.isfile(db_name):
        return
        
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    create_user = 'CREATE TABLE user(id INTEGER PRIMARY KEY, name TEXT, location TEXT);'
    cur.execute(create_user)
            
    create_tweet = 'CREATE TABLE tweet(id INTEGER PRIMARY KEY, time TEXT, userid INTEGER, content TEXT, url TEXT, url_domain TEXT, favs INTEGER, retweets INTEGER, FOREIGN KEY(userid) REFERENCES user(id));'
    cur.execute(create_tweet)
    
    create_mention = 'CREATE TABLE mention(userid INTEGER, tweetid INTEGER, PRIMARY KEY (userid, tweetid), FOREIGN KEY(userid) REFERENCES user(id), FOREIGN KEY(tweetid) REFERENCES tweet(id));'
    cur.execute(create_mention)
    
    create_hashtag = 'CREATE TABLE hashtag(tweetid INTEGER, tag TEXT, PRIMARY KEY(tweetid, tag), FOREIGN KEY(tweetid) REFERENCES tweet(id))';
    cur.execute(create_hashtag);
    
    conn.commit()
    conn.close()
    

#
# Insert user into database
#
def insertUser(user):
    if not os.path.isfile(db_name):
        print "cannot open db"
        return
        
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    insert_user = 'INSERT INTO user VALUES(?, ?, ?)'
    
    location = user.get('location', '')
    
    try:
        cur.execute(insert_user, (user['id'], user['screen_name'], location))
    except sqlite3.IntegrityError:
        print "user already exists. ignore it"
    finally:
        conn.commit()
        conn.close()

#
# Insert tweet into database
#
def insertTweet(tweet_id, time, userid, content, url, url_domain, hashtags, favs, retweets):
    if not os.path.isfile(db_name):
        print "cannot open db"
        return
        
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    

    insert_tweet = 'INSERT INTO tweet VALUES(?, ?, ?, ?, ?, ?, ?, ?);'
    
    insert_hashtag = 'INSERT INTO hashtag VALUES(?, ?);'
  
    try:
        cur.execute(insert_tweet, (tweet_id, time, userid, content, url, url_domain, favs, retweets))
        
        for tag in hashtags:
            cur.execute(insert_hashtag, (tweet_id, tag['text']))
        
    except sqlite3.IntegrityError:
        print "tweet already exists. ignore it"
    finally:
        conn.commit()
        conn.close()
    
#
# insert mention into database
#
def insertMention(userid, tweetid):
    if not os.path.isfile(db_name):
        print "cannot open db"
        return
        
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    insert_mention = 'INSERT INTO mention VALUES(?, ?)'
    
    try:
        cur.execute(insert_mention, (userid, tweetid))
    except sqlite3.IntegrityError:
        print "mention cannot insert"
    finally:
        conn.commit()
        conn.close()

#
# resolve url form it's short form (like t.co) to its original from
#
def unshorten_url(url):

    try:
        parsed = urlparse(url)
        h = httplib.HTTPConnection(parsed.netloc, timeout=10)
        h.timeout
        h.request('HEAD', parsed.path)
        response = h.getresponse()
        if response.status/100 == 3 and response.getheader('Location'):
            return response.getheader('Location')
        else:
            return url
    except Exception as e:
        #print e
        print 'could not unshorten'

#
# resolve url until its original form has been reached
# (some urls are shortened multiple times)
#
def unshorten_till_done(url):
    
    max_depth = 5;
    
    cur_url = url
    new_url = unshorten_url(url)
    
    while ((new_url != cur_url) and (max_depth > 0)):
        cur_url = new_url
        new_url = unshorten_url(cur_url)
    
    return cur_url
    
#
# get the url included in the tweet
#
def extractUrl(tweet):
    s = re.search("(?P<url>https?://[^\s]+)", tweet)
    if (s is not None):
        result = s.group("url")
        return result
   
#
# from a complete url, just return the domain
#
def getDomain(url):
    parsed_uri = urlparse(url)
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    return domain


#
# search for the keywords and put the result into the database
#
def crawl(keywords):

    try:
        tso = TwitterSearchOrder() # create a TwitterSearchOrder object
        tso.set_keywords(keywords) # let's define all words we would like to have a look for
        tso.set_include_entities(True) # and don't give us all those entity information

        # it's about time to create a TwitterSearch object with our secret tokens
        ts = TwitterSearch(
            consumer_key = _consumer_key,
            consumer_secret = _consumer_secret,
            access_token = _access_token,
            access_token_secret = _access_token_secret
         )
        
        
        
        while True: 
            # this is where the fun actually starts :)
            for tweet in ts.search_tweets_iterable(tso):
                print( '%s: @%s tweeted: %s' % (tweet['created_at'], tweet['user']['screen_name'], tweet['text'] ) )
               
                user = tweet['user']
                insertUser(user);
                
                created_str = str(tweet['created_at'])
                created = datetime.strptime(created_str, '%a %b %d %H:%M:%S +0000 %Y')
                content = tweet['text'] 
                hashtags = tweet['entities']['hashtags']
       
       
                url = extractUrl(content)
                url = unshorten_till_done(url)
                           
                try:
                    url_domain = getDomain(url)

                except:
                    url = None
                    url_domain = None
                    
                    
                favs = tweet['favorite_count']
                retweets = tweet['retweet_count']
                
                insertTweet(tweet['id'], created, user['id'], content, url, url_domain, hashtags, favs, retweets);
                
                mentions = tweet['entities']['user_mentions']
                
                for mention in mentions:
                    insertUser(mention);
                    insertMention(mention['id'], tweet['id'])
                    
            print "query new results"
            ts.search_next_results()
            
    except TwitterSearchException as e: # take care of all those ugly errors if there are some
        print(e)


def main():
   init()
   
   #crawl for multiple keywords
   crawl(['#keyword1', '-#notkeyword2'])


if __name__ == "__main__":
    main()
