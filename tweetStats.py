#!/usr/bin/env python3

import tweepy
import pathlib
import configparser


def validateConfig(config_file):
    "Validate that the config file exists and is a file."
    return pathlib.Path(config_file).is_file()


def parseConfig(config_file):
    "Parse the config file and return user keys."

    try:
        validateConfig(config_file)
    except FileNotFoundError:
        raise 'unable to find {}'.format(config_file)

    parser = configparser.ConfigParser()
    parser.read(config_file)
    config = dict((section, dict((option, parser.get(section, option))
                  for option in parser.options(section)))
                  for section in parser.sections())

    return(config)


def twitterApi(api_key, api_secret, access_token, access_secret):
    "Authenticate and create a Twitter session."

    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_secret)

    return tweepy.API(auth)


def getUser(twitter_api, user):
    return twitter_api.get_user(user)


def main():
    "Do the main."
    data = parseConfig('tstats.conf')

    twitter = twitterApi(data['api']['key'],
                         data['api']['secret'],
                         data['access']['token'],
                         data['access']['secret'])

    user = getUser(twitter, data['user']['username'])

    print(user.followers_count)


if __name__ == "__main__":
    main()
