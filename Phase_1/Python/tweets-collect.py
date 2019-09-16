import tweepy #Import tweepy
import csv #Import csv
# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="RiAljDExhBmC58Iqh0fyv3Kia"
consumer_secret="4Bo2UzGErfBGJARHVBA0zkrA3pLCxgj916MwDJzwbdp3PwVUFn"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section

access_token="391206417-Ws9xmwz1zfJBGD6dlj7RNej8EroI3e26kRpwov5d"
access_token_secret="LDbm2pGU7IMQ0QiACTmnokPLePxfSW01eR5K8R64YNTQy"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Open/Create a file to append data
csvFile = open('Writetweetsdata.csv', 'a')
# Use csv Writer
csvWriter = csv.writer(csvFile)

api = tweepy.API(auth)

public_tweets = api.home_timeline()
#for tweet in public_tweets:
 #   print(tweet.text)

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        try:
            csvWriter.writerow([status.created_at, status.entities, status.text.encode('utf-8')])
            #csvWriter.writerow([status._json])
            print(status._json)

        except BaseException as e:
            print('problem collecting tweet', str(e))
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

myStream.filter(track=['pulwama'])

csvFile.close()