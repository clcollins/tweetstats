FROM python:3.7-alpine

LABEL maintainer 'Chris Collins <collins.christopher@gmail.com'

RUN apk add git \
      && pip3 install git+https://github.com/tweepy/tweepy.git@2efe385fc69385b57733f747ee62e6be12a1338b configparser --user --no-cache-dir

COPY tweetStats.py /tweetStats.py

ENTRYPOINT [ "/tweetStats.py" ]

