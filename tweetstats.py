#!/usr/bin/env python3

import os
import tweepy
from datetime import datetime
from influxdb import InfluxDBClient


def parseConfig():
    keys = ['api_key',
            'api_secret',
            'access_token',
            'access_secret',
            'username',
            'INFLUXDB_HOST',
            'INFLUXDB_DATABASE',
            'INFLUXDB_USER',
            'INFLUXDB_PASSWORD'
            ]

    data = {}

    for k in keys:
        if k not in os.environ:
            raise Exception('{} not found in environment'.format(k))
        else:
            data[k] = os.environ[k]

    return(data)


def twitterApi(api_key, api_secret, access_token, access_secret):
    "Authenticate and create a Twitter session."

    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_secret)

    return tweepy.API(auth)


def getUser(twitter_api, user):
    return twitter_api.get_user(user)


def createInfluxDB(client, db_name):
    "Create the database if it doesn't exist."
    dbs = client.get_list_database()
    if not any(db['name'] == db_name for db in dbs):
        client.create_database(db_name)
    client.switch_database(db_name)


def initDBClient(host, db, user, password):
    "Create an InfluxDB client connection"

    client = InfluxDBClient(host, 8086, user, password, db)

    return(client)


def createPoint(username, measurement, value, time):
    "Create a datapoint."
    json_body = {
        "measurement": measurement,
        "tags": {
            "user": username
        },
        "time": time,
        "fields": {
            "value": value
        }
    }

    return json_body


def main():
    "Do the main."
    data = parseConfig()
    time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    twitter = twitterApi(data['api_key'],
                         data['api_secret'],
                         data['access_token'],
                         data['access_secret'])

    user = getUser(twitter, data['username'])

    client = initDBClient(data['INFLUXDB_HOST'],
                          data['INFLUXDB_DATABASE'],
                          data['INFLUXDB_USER'],
                          data['INFLUXDB_PASSWORD'])

    createInfluxDB(client, data['INFLUXDB_DATABASE'])

    json_body = []

    data_points = {
        "followers_count": user.followers_count,
        "friends_count": user.friends_count,
        "listed_count": user.listed_count,
        "favourites_count": user.favourites_count,
        "statuses_count": user.statuses_count
    }

    for key, value in data_points.items():
        json_body.append(createPoint(data['username'], key, value, time))

    print(json_body)
    client.write_points(json_body)


if __name__ == "__main__":
    main()
