import tweepy
import socket
from tweepy import OAuthHandler
from datetime import datetime

access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        conn.send((status.text + '\n').encode())


class TimeoutException(Exception):
    pass


if __name__ == '__main__':
    # Create a TCP/IP socket
    TCP_IP = "localhost"
    TCP_PORT = 10000
    # Create a socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((TCP_IP, TCP_PORT))
    print("waiting for connection....")
    sock.listen(1)
    conn, addr = sock.accept()


    while True:
        try:
        #This handles Twitter authetification and the connection to Twitter Streaming API
            auth = OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)

            api = tweepy.API(auth)

        #creating stream listner
            myStreamListener = MyStreamListener()
            myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
            myStream.filter(languages=["en"], track=['Python', 'Data Analytics', 'BigData', 'Hadoop'])

        except KeyboardInterrupt:
            print("%s - KeyboardInterrupt caught. Closing stream and exiting.")
            myStreamListener.close()
            myStream.disconnect()
            break

        except TimeoutException:
            print("%s - Timeout exception caught. Closing stream and reopening." % datetime.now())
            try:
                myStreamListener.close()
                myStream.disconnect()
            except:
                pass