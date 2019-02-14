#!/usr/bin/env python3

import tweepy
import argparse
import configparser
import mysql.connector
from influxdb import InfluxDBClient
from datetime import datetime, timedelta


def parseConfig(configfile):
    """Get authentication data from ENV variables."""
    config = configparser.ConfigParser()
    config.read(configfile)

    return config


def initAPI(creds):
    """Authenticate and create an API instance."""

    auth = tweepy.OAuthHandler(creds['consumer_key'],
                               creds['consumer_secret'])

    auth.set_access_token(creds['access_token'],
                          creds['access_token_secret'])

    return tweepy.API(auth,
                      wait_on_rate_limit=True,
                      wait_on_rate_limit_notify=True,
                      retry_count=10, retry_delay=5,
                      retry_errors=5)


def initInfluxDB(creds):
    """Setup the InfluxDB connection."""
    conn = InfluxDBClient(host=creds['host'],
                          username=creds['user'],
                          password=creds['password'])

    return conn


def initMYSQL(creds):
    """Setup MySQL db connection."""
    conn = mysql.connector.connect(
           host=creds['host'],
           user=creds['user'],
           passwd=creds['password']
    )

    return conn


def getCurrentFollowers(api, count=None):
    """Get the authenticated user's followers and return a dict with data."""
    # Gather data for authenticated user.
    user = api.me()

    if count == -1:
        followers = tweepy.Cursor(api.followers, id=user.id).items()
    else:
        followers = tweepy.Cursor(api.followers, id=user.id).items(count)

    # Create a dictionary to parse into database columns
    gather = {}
    for follower in followers:
        gather[follower] = {
            'id': follower.id,
            'name': follower.name,
            'screen_name': follower.screen_name,
            'twitter_json': follower._json
        }

    return gather


def storeFollowers(connection, database, followers):
    """Parse follower data and put it into the db."""
    table = 'followers'
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    yesterday = ((datetime.now() - timedelta(days=1))
                 .strftime('%Y-%m-%d %H:%M:%S'))
    cursor = connection.cursor()
    cursor.execute("SET NAMES utf8mb4")
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database))
    cursor.execute("CREATE TABLE IF NOT EXISTS {}.{}"
                   " (id varchar(70) primary key, screen_name varchar(70),"
                   " name varchar(70), twitter_json JSON,"
                   " first_seen datetime, last_seen datetime,"
                   " gone boolean);".format(database, table))

    for follower in followers:
        print(follower.screen_name)
        sql = ("INSERT INTO {}.{}"
               " (id,screen_name,name,"
               # "twitter_json,"
               "first_seen,last_seen)"
               " VALUES (%s,%s,"
               # "'{json}',"
               "%s,%s,%s)"
               " ON DUPLICATE KEY UPDATE"
               " screen_name=%s,"
               " name=%s,"
               # " twitter_json='{json}',"
               " last_seen=%s").format(database, table)
        cursor.execute(sql, (follower.id,
                             follower.screen_name,
                             follower.name,
                             now, now,
                             follower.screen_name,
                             follower.name,
                             now))
        # connection.commit()

        sql = ("SELECT last_seen from {database}.{table}"
               " WHERE id={id}").format(database=database,
                                        table=table,
                                        id=follower.id)
        cursor.execute(sql)
        last_seen = cursor.fetchone()[0]

        # If the last_seen value is less than (before) yesterday, they can
        # be considered to have unfollowed
        if last_seen.strftime('%Y-%m-%d %H:%M:%S') < yesterday:
            sql = ("UPDATE {database}.{table}"
                   " SET gone = 1 WHERE id = '{id}'"
                   .format(database=database, table=table, id=follower.id))
            cursor.execute(sql)
        else:
            sql = ("UPDATE {database}.{table}"
                   " SET gone = 0 WHERE id = '{id}'"
                   .format(database=database, table=table, id=follower.id))
            cursor.execute(sql)

    connection.commit()
    cursor.close()
    connection.close()


def processFollowers(args):
    """Authenticate and gather followers."""
    creds = parseConfig(args.configfile)
    followers = getCurrentFollowers(initAPI(creds['twitter']), args.count)
    mysql = initMYSQL(creds['mysql'])
    storeFollowers(mysql, creds['mysql']['database'], followers)

    # sudo podman run -e MYSQL_ROOT_PASSWORD=root \
    #                 -p 127.0.0.1:3306:3306 -it docker.io/mariadb:10.4


def getMetricsCount(api):
    """Get metrics for the user from the Twitter API."""

    user = api.me()

    data_points = {
        "followers_count": user.followers_count,
        "friends_count": user.friends_count,
        "listed_count": user.listed_count,
        "favourites_count": user.favourites_count,
        "statuses_count": user.statuses_count
    }

    return user.screen_name, data_points


def storeMetrics(connection, database, username, metrics):
    """Parse follower data and put it into the db."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dbs = connection.get_list_database()

    if not any(db['name'] == database for db in dbs):
        connection.create_database(database)
    connection.switch_database(database)

    json_body = []

    for key, value in metrics.items():
        json_body.append(createPoint(username, key, value, now))

    connection.write_points(json_body)


def createPoint(username, measurement, value, time):
    "Create a datapoint."
    datapoint = {
        "measurement": measurement,
        "tags": {
            "user": username
        },
        "time": time,
        "fields": {
            "value": value
        }
    }

    return datapoint


def processMetrics(args):
    """Authenticate and gather metrics."""
    creds = parseConfig(args.configfile)
    username, metrics = getMetricsCount(initAPI(creds['twitter']))
    influxdb = initInfluxDB(creds['influxdb'])
    storeMetrics(influxdb, creds['influxdb']['database'], username, metrics)


def main():
    """Parse command line arguments and continue on."""
    parser = argparse.ArgumentParser(description='Gather Twitter Stats')
    parser.add_argument('-f', '--configfile', action='store',
                        default='.tstats.cfg',
                        help='Path to alternate configuration file')

    subparsers = parser.add_subparsers(title='subcommands')

    metrics = subparsers.add_parser(
        'metrics',
        description=('Gather your account and post to InfluDB for graphing'))
    metrics.set_defaults(func=processMetrics)

    followers = subparsers.add_parser(
        'followers',
        description=('Gather current follower information and store in MySQL'))
    followers.add_argument('-c', '--count', action='store',
                           type=int, default=-1,
                           help='Limit API query for followers to X number.')
    followers.set_defaults(func=processFollowers)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
