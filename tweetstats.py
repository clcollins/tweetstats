#!/usr/bin/env python3

import tweepy
import argparse
import configparser
import mysql.connector
from influxdb import InfluxDBClient
from datetime import datetime, timedelta


def getTheTime(delta=0, raw=False):
    """Get formatted dates from 'now'."""

    if raw:
        return (datetime.now() + timedelta(days=delta))
    else:
        return ((datetime.now() + timedelta(days=delta))
                .strftime('%Y-%m-%d %H:%M:%S'))


def initAPI(creds, verbose=False):
    """Authenticate and create an API instance."""
    if verbose:
        print("Initializing Twitter API Client")

    auth = tweepy.OAuthHandler(creds['consumer_key'],
                               creds['consumer_secret'])

    auth.set_access_token(creds['access_token'],
                          creds['access_token_secret'])

    return tweepy.API(auth,
                      wait_on_rate_limit=True,
                      wait_on_rate_limit_notify=True,
                      retry_count=10, retry_delay=5,
                      retry_errors=5)


def initInfluxDB(creds, verbose=False):
    """Setup the InfluxDB connection."""
    if verbose:
        print("Initializing InfluxDB Client")

    conn = InfluxDBClient(host=creds['host'],
                          username=creds['user'],
                          password=creds['password'])

    return conn


def initMYSQL(creds, verbose=False):
    """Setup MySQL db connection."""
    if verbose:
        print("Initializing MySQL Client")

    conn = mysql.connector.connect(
           host=creds['host'],
           user=creds['user'],
           passwd=creds['password']
    )

    return conn


def getCurrentFollowers(api, count=None, verbose=False):
    """Get the authenticated user's followers and return a dict with data."""
    # Gather data for authenticated user.
    user = api.me()

    if verbose:
        print(("Gathering follower data for {user}; "
               "number to gather = {count}"
               .format(user=user.screen_name, count=count)))

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


def getUnfollowers(connection, database, table, verbose=False):
    """Count and post folks who unfollowed today."""
    debug = True

    if verbose:
        print("Counting and saving unfollow stats")

    cursor = connection.cursor()
    cursor.execute("SET NAMES utf8mb4")
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database))

    sql = ("CREATE TABLE IF NOT EXISTS {}.{}"
           " (id varchar(70) primary key, screen_name varchar(70),"
           " name varchar(70), twitter_json longtext,"
           " first_seen datetime, last_seen datetime,"
           " gone boolean);".format(database, table))

    if debug:
        print(sql)

    cursor.execute(sql)
    connection.commit()

    # Drop hours/minutes
    day_before_yesterday = getTheTime(-2, raw=True).date()

    # They left yesterday if last_seen is between two days ago and yesterday
    sql = ("SELECT id, screen_name "
           "FROM {}.{} "
           "WHERE last_seen < '{}' "
           "AND gone = 0;".format(database, table, day_before_yesterday))

    if debug:
        print(sql)

    cursor.execute(sql)
    unfollowers = cursor.fetchall()
    count = len(unfollowers)

    if verbose:
        print("Unfollowers count: {}".format(count))

    cursor.close()

    return count, unfollowers


def storeFollowers(connection, database, table, followers, verbose=False):
    """Parse follower data and put it into the db."""
    debug = False

    if verbose:
        print("Storing follower information in MariaDB")

    cursor = connection.cursor()
    cursor.execute("SET NAMES utf8mb4")
    cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database))

    sql = ("CREATE TABLE IF NOT EXISTS {}.{}"
           " (id varchar(70) primary key, screen_name varchar(70),"
           " name varchar(70), twitter_json longtext,"
           " first_seen datetime, last_seen datetime,"
           " gone boolean);".format(database, table))

    if debug:
        print(sql)

    cursor.execute(sql)

    for follower in followers:
        if verbose:
            print("Storing {follower}".format(follower=follower.screen_name))

        now = getTheTime(0)

        sql = ("INSERT INTO {}.{}"
               " (id,screen_name,name,"
               # "twitter_json,"
               "first_seen,last_seen, gone)"
               " VALUES (%s,%s,"
               # "'{json}',"
               "%s,%s,%s, 0)"
               " ON DUPLICATE KEY UPDATE"
               " screen_name=%s,"
               " name=%s,"
               # " twitter_json='{json}',"
               " last_seen=%s, gone=0").format(database, table)
        cursor.execute(sql, (follower.id,
                             follower.screen_name,
                             follower.name,
                             now, now,
                             follower.screen_name,
                             follower.name,
                             now))

    connection.commit()
    cursor.close()


def storeUnfollowers(connection, database, table, unfollowers, verbose=False):
    """Parse unfollower data and put it into the db."""

    if verbose:
        print("Storing unfollower information in MariaDB")

    cursor = connection.cursor()

    for unfollower in unfollowers:
        if verbose:
            print("Storing {unfollower}".format(unfollower=unfollower[1]))

        sql = ("UPDATE {}.{}"
               " SET gone = 1"
               " WHERE id = {}").format(database, table, unfollower[0])

        cursor.execute(sql, (unfollower[0]))

    connection.commit()
    cursor.close()


def processFollowers(args):
    """Authenticate and gather followers."""
    if args.verbose:
        print("Processing follower data")

    table = 'followers'

    followers = getCurrentFollowers(initAPI(args.creds['twitter'],
                                            args.verbose),
                                    args.count,
                                    args.verbose)
    mysql = initMYSQL(args.creds['mysql'], args.verbose)
    storeFollowers(mysql, args.creds['mysql']['database'],
                   table, followers, args.verbose)

    # Close conn
    mysql.close()


def processUnfollowers(args):
    """Count and store unfollowers since yesterday"""
    if args.verbose:
        print("Processing unfollower data")

    table = 'followers'

    username = (initAPI(args.creds['twitter'], args.verbose)).me().screen_name
    mysql = initMYSQL(args.creds['mysql'], args.verbose)
    count, unfollowers = getUnfollowers(mysql, args.creds['mysql']['database'],
                                        table, args.verbose)

    storeUnfollowers(mysql, args.creds['mysql']['database'],
                     table, unfollowers, args.verbose)
    # Close conn
    mysql.close()

    influxdb = initInfluxDB(args.creds['influxdb'], args.verbose)
    storeUnfollowerCount(influxdb,
                         args.creds['influxdb']['database'],
                         username,
                         count,
                         args.verbose)


def storeUnfollowerCount(connection, database, username, count, verbose=False):
    """Create a datapoint and write the unfollower count into InfluxDB"""
    debug = True

    if verbose:
        print("Writing the unfollower count to InfluxDB: {}".format(count))

    json_body = []
    json_body.append(createPoint(username, 'unfollows', count,
                                 getTheTime(0), verbose))

    if verbose:
        print("Writing metrics data to InfluxDB")

    if debug:
        print(json_body)

    dbs = connection.get_list_database()

    if not any(db['name'] == database for db in dbs):
        if verbose:
            print(("Influx database {} does not exist; "
                   "creating it".format(database)))
        connection.create_database(database)
    connection.switch_database(database)

    connection.write_points(json_body)


def getMetricsCount(api, verbose=False):
    """Get metrics for the user from the Twitter API."""
    user = api.me()

    if verbose:
        print("Gathering metrics for {user}".format(user=user.screen_name))

    data_points = {
        "followers_count": user.followers_count,
        "friends_count": user.friends_count,
        "listed_count": user.listed_count,
        "favourites_count": user.favourites_count,
        "statuses_count": user.statuses_count
    }

    return user.screen_name, data_points


def storeMetrics(connection, database, username, metrics, verbose=False):
    """Parse follower data and put it into the db."""
    debug = True

    if verbose:
        print("Storing metrics data")

    dbs = connection.get_list_database()

    if not any(db['name'] == database for db in dbs):
        if verbose:
            print(("Influx database {} does not exist; "
                   "creating it".format(database)))
        connection.create_database(database)
    connection.switch_database(database)

    json_body = []

    for key, value in metrics.items():
        json_body.append(createPoint(username, key, value,
                                     getTheTime(0), verbose))

    if verbose:
        print("Writing metrics data to InfluxDB")

    if debug:
        print(json_body)

    connection.write_points(json_body)


def createPoint(username, measurement, value, time, verbose=False):
    "Create a datapoint."
    if verbose:
        print(("Creating datapoint from {measurement}: {value}"
               .format(measurement=measurement, value=value)))

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
    if args.verbose:
        print("Processing Twitter metrics")
    username, metrics = (getMetricsCount(initAPI(args.creds['twitter'],
                                         args.verbose), args.verbose))
    influxdb = initInfluxDB(args.creds['influxdb'], args.verbose)
    storeMetrics(influxdb,
                 args.creds['influxdb']['database'],
                 username,
                 metrics,
                 args.verbose)


def main():
    """Parse command line arguments and continue on."""
    parser = argparse.ArgumentParser(description='Gather Twitter Stats')
    parser.add_argument('-f', '--configfile', action='store',
                        default='.tstats.cfg',
                        help='Path to alternate configuration file')
    parser.add_argument('-v', '--verbose', action='store_true',
                        default=False,
                        help='Enable verbose output')

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

    unfollowers = subparsers.add_parser(
        'unfollows',
        description=('Count and store unfollow information since yesterday'))
    unfollowers.set_defaults(func=processUnfollowers)

    args = parser.parse_args()
    args.creds = configparser.ConfigParser().read(args.configfile)
    args.func(args)


if __name__ == "__main__":
    main()
