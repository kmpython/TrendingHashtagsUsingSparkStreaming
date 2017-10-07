# TrendingHashtagsUsingSparkStreaming

In this project I have attempted to keep track of the trending hashtags on twitter on the topics of Python, Data Analytics, Hadoop and Big Data using Spark Streaming

The whole process is divided into two parts -
* The Twitter App : This python application uses tweepy to fetch tweets from twitter. We then pass the tweets to `localhost:10000` for further processing
* The Spark Streaming App: This app streams data recieved from `localhost:10000` and keeps a count of the hashtags

To run this app I am using 
```
Python version - 3.5.2 and
Spark version - 2.1.0
```
on my windows machine

references:
* https://stackoverflow.com/questions/27014955/socket-connect-vs-bind
* https://www.toptal.com/apache/apache-spark-streaming-twitter
* https://docs.python.org/3/howto/sockets.html
* https://spark.apache.org/docs/latest/streaming-programming-guide.html
