import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import re
import io
import csv

consumer_key = 'sz6x0nvL0ls9wacR64MZu23z4'
consumer_secret = 'ofeGnzduikcHX6iaQMqBCIJ666m6nXAQACIAXMJaFhmC6rjRmT'
access_token = '854004678127910913-PUPfQYxIjpBWjXOgE25kys8kmDJdY0G'
access_token_secret = 'BC2TxbhKXkdkZ91DXofF7GX8p2JNfbpHqhshW1bwQkgxN'

#(Tweet_Listener class) uses the 4 authentication keys to create the connection with twitter, extract the feed and channelize them using Socket
#Responsible for the streaming itself:
#on_data method is responsible for receiving data from the Twitter stream and sending it to a socket
#on_error method is used for receiving error messages
class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
  def on_status(self, status):
    if status.retweeted_status or status.favorite_count is None: #or status.favorite_count < 10:
      return
  def on_data(self, data):
    try:
      msg = json.loads( data )
      print( msg['text'].encode('utf-8') )
      self.client_socket.send( msg['text'].encode('utf-8') )
      return True
    except BaseException as e:
      print("Error on_data: %s" % str(e))
      return True
  def on_error(self, status):
    if status == 420:
        return False
    else:
        print(status)
        return True

#connect to Twitter streaming
#takes socket object as a parameter
def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=["musk", "elon", "elon musk"], languages=["en"])

# #create a socket object and start the streaming process
s = socket.socket()
host = '127.0.0.3'
port = 5555
addr = (host,port)
s.bind(addr)
print("Listening on port: %s" % str(port))
s.listen(5)
c, addr = s.accept()
print("Received request from: " + str(addr))
sendData(c)

# auth = OAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(access_token, access_token_secret)
# twitter_stream = Stream(auth, TweetsListener(s))
# twitter_stream.filter(track=["musk", "elon", "elon musk"], languages=["en"])
