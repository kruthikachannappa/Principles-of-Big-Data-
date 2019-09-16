from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key = "RiAljDExhBmC58Iqh0fyv3Kia"
consumer_secret = "4Bo2UzGErfBGJARHVBA0zkrA3pLCxgj916MwDJzwbdp3PwVUFn"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section

access_token = "391206417-Ws9xmwz1zfJBGD6dlj7RNej8EroI3e26kRpwov5d"
access_token_secret = "LDbm2pGU7IMQ0QiACTmnokPLePxfSW01eR5K8R64YNTQy"


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    def on_data(self, data):
        try:
            with open('data3.json', 'a') as outfile:
                json.dump(data, outfile)
            with open('data2.json', 'a') as outputj:
                outputj.write(data)
            with open('tweets.txt', 'a') as tweets:
                tweets.write(data)
                tweets.write('\n')
            outfile.close()
            tweets.close()
            outputj.close()
        except BaseException as e:
            print('problem collecting tweet', str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['bjp',"congress party",'Bharatiya Janata Party' 'Indian National Congress', 'aap', 'bjd', 'tdp', 'tmc', 'Samajwadi Party', 'Bahujan Samaj Party0',
                   'Narendra Modi', 'Rahul Gandhi', 'Arvind kejriwal', 'Naveen Pattnaik', 'Akhilesh Yadav',
                   'Chandrababu Naidu', 'ElectionsIndia', 'IndianElections', 'LoksabhaElections2019', 'PMO', 'Namo',
                   'RaGa', 'Loksabha', 'Rajyasabha', 'Varanasi', 'Amethi', 'NDA', 'UPA', 'chowkidar',
                   'Mamata Banerjee'])
