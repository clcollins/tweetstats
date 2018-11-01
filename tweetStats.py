#!/usr/bin/env python3

import os
import tweepy


def parseConfig():
    keys = ['api_key',
            'api_secret',
            'access_token',
            'access_secret',
            'username']

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


def main():
    "Do the main."
    data = parseConfig()

    twitter = twitterApi(data['api_key'],
                         data['api_secret'],
                         data['access_token'],
                         data['access_secret'])

    user = getUser(twitter, data['username'])

    print(user.followers_count)


if __name__ == "__main__":
    main()
