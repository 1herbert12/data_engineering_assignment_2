from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from pandas import DataFrame as DF

access_token = "269718749-Hqx2DhMXMNIw7J0A9jIRIV51RTZApRSDFtJxkLbE"
access_token_secret = "FsS2yKKfNJLQAmX3XOXzPjtklfAeCLFs8curWxL0s"
consumer_key = "kM643q7ikQbNn2Np2Ff0NA"
consumer_secret = "Ig5trlvjENWmTh0IWJL5JEavX61FA4ALCrHwpU0"


class StdOutListener(StreamListener):
    def parse_tweet(self, tweet):

        message = {'sender': tweet['user']['name'], 'receiver': None,
                   'timestamp_ms': tweet['timestamp_ms'] if 'timestamp_ms' in tweet else None,
                   'message': tweet['text'] if 'text' in tweet else None, 'hashtags': []}

        if 'in_reply_to_screen_name' in tweet:
            message['receiver'] = tweet['in_reply_to_screen_name']

        if 'entities' in tweet:
            entities = tweet['entities']
            if 'hashtags' in entities:
                message['hashtags'] = entities['hashtags']

        if 'extended_tweet' in tweet:
            message['text'] = tweet['extended_tweet']['text']

        if 'quoted_status' in tweet:
            message['reply_to'] = self.parse_tweet(tweet['quoted_status'])

        return message


    def on_data(self, data_str):
        data = json.loads(data_str)
        message = self.parse_tweet(data)

        print(message)

        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # This handles Twitter authetification and the connection to Twitter Streaming API
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)

    # This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(follow=['822215679726100480'])
