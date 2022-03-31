import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import time


def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())



def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(clean_tweet(tweet))
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

def sentiment_analysis(api, tweets):
    positive_tweet_cnt = 0
    positive_tweets = []
    
    negative_tweet_cnt = 0
    negative_tweets = []
    
    neutral_tweet_cnt = 0
    neutral_tweets = []
    
    array_dict = {'Username':[], 'Followers':[], 'Tweets':[],
                  'Retweets':[], 'Text':[], 'Date':[],
                  'Location':[], 'Hashtags':[], 'Sentiment':[]}
    
        
    cnt = 0
    for tweet in tweets:
        array_dict['Username'].append(tweet.user.screen_name) # get username of person that wrote the tweet
        print(array_dict['Username'][cnt])
        cnt += 1
        
        # Get the followers of that person
        array_dict['Followers'].append(tweet.user.followers_count)
        #print(array_dict['Followers'])
        
        # Get number of tweets
        try:
            array_dict['Tweets'].append(api.get_user(tweet.user.screen_name).statuses_count)
        except:
            array_dict['Tweets'].append('user_not_found')
        #print(f'statuses_count:  {array_dict["Tweets"]}')
        
        # Get number of retweets
        array_dict['Retweets'].append(tweet.retweet_count)
        #print(f'retweets {array_dict["Retweets"]}')
        
        # Get the text of the tweet
        array_dict['Text'].append(tweet.text)
        #print(f'Text: {array_dict["Text"]}')
        
        # Get the date of the tweet
        array_dict['Date'].append(str(tweet.created_at.date()))
        #print(f'Date: {array_dict["Date"]}')
        
        # Get user location
        # It returns the location only with the user's id
        try:
            user = api.get_user(tweet.user.screen_name)
            user_id = user.id_str
            user = api.get_user(int(user_id))
            array_dict['Location'].append(user.location)
        except:
            array_dict['Location'].append('user_not_found')
        #print(f'Location: {array_dict["Location"]}')
        
        # Get the hashtags of the tweet. Sometimes there are none
        array_dict['Hashtags'].append(' '.join([hashtag for hashtag in str(tweet).split() if hashtag.startswith('#')]))
        #print(f"Hashtags: {array_dict['Hashtags']}")
        
        
        # At the following lines we get the sentiment of each tweet from it's text
        if get_tweet_sentiment(tweet.text) == 'positive':
            positive_tweet_cnt += 1
            array_dict['Sentiment'].append('positive')
            positive_tweets.append(tweet.text)
            #print(positive_tweets[0])
            
            
        elif get_tweet_sentiment(tweet.text) == 'neutral':
            neutral_tweet_cnt += 1
            array_dict['Sentiment'].append('neutral')
            neutral_tweets.append(tweet.text)
        else:
            negative_tweet_cnt += 1
            array_dict['Sentiment'].append('negative')
            negative_tweets.append(tweet.text)
    
    df = pd.DataFrame(array_dict) # Create a pandas dataframe with the dictionary we created above
     
        
    # percentage of positive tweets        
    print("Positive tweets percentage: {} %".format(100*len(positive_tweets)/1000))
    
    # percentage of negative tweets
    print("Negative tweets percentage: {} %".format(100*len(negative_tweets)/1000))
    
    # percentage of neutral tweets
    print("Neutral tweets percentage: {} %".format(100*len(neutral_tweets)/1000))
      
    # printing first 5 positive tweets
    print("\n###########POSITIVE TWEETS###########\n\nPositive tweets:")
    for tweet in positive_tweets[:5]:
        print(tweet)
      
    # printing first 5 negative tweets
    print("\n###########NEGATIVE TWEETS###########\n\nNegative tweets:")
    for tweet in negative_tweets[:5]:
        print(tweet)
      
    return df # return the dataframe we created with the information of each tweet


def get_tweets(api, context):
    
    # create OAuthHandler object
    auth = OAuthHandler(consumer_key, consumer_secret)
    # set access token and secret
    auth.set_access_token(access_token, access_token_secret)
    # create tweepy API object to fetch tweets
    api = tweepy.API(auth, wait_on_rate_limit=True)
    tweets = tweepy.Cursor(api.search, q=context, since='2022-01-01', 
                           until='2022-02-08', count=200).items(1000)

    return tweets
    



def insert_to_database(df):
    # SOURCE: https://www.dataquest.io/blog/sql-insert-tutorial/
    # create a connection to the database
    connection = pymysql.connect(host='localhost',
        user='root',
        password='1234',
        db='twitter_analysis_v2')
        
    # create cursor
    cursor=connection.cursor()
    
    
    # creating column list for insertion
    cols = "`,`".join([str(i) for i in df.columns.tolist()])
    
    # Insert DataFrame recrds one by one.
    for i,row in df.iterrows():
        sql = "INSERT INTO `twitter_analysis_v2` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))
    
        # the connection is not autocommitted by default, so we must commit to save our changes
        connection.commit()

    # Execute query
    print('##### EXECUTE QUERY #####')
    sql = "SELECT * FROM `twitter_analysis_v2` LIMIT 1"
    cursor.execute(sql)
    
    # Fetch all the records
    result = cursor.fetchall()
    for i in result:
        print(i)
        
    print('#######################')




consumer_key = '---'
consumer_secret = '---'
access_token = '---'
access_token_secret = '---'
  
# attempt authentication

# create OAuthHandler object
auth = OAuthHandler(consumer_key, consumer_secret)
# set access token and secret
auth.set_access_token(access_token, access_token_secret)
# create tweepy API object to fetch tweets
api = tweepy.API(auth, wait_on_rate_limit=True)


tweets_1 = get_tweets(api, 'Donald Trump')
df_1 = sentiment_analysis(api, tweets_1)
insert_to_database(df_1)



tweets_2 = get_tweets(api, 'Giannis Antetokoubo')
df_2 = sentiment_analysis(api, tweets_2)
insert_to_database(df_2)


tweets_3 = get_tweets(api, 'Lakers')
df_3 = sentiment_analysis(api, tweets_3)
insert_to_database(df_3)








