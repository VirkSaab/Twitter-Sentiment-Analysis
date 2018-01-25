register '/home/virksaab/Desktop/TwitterSentimentAnalysis/pig_jars/elephant-bird-hadoop-compat-4.1.jar'
register '/home/virksaab/Desktop/TwitterSentimentAnalysis/pig_jars/elephant-bird-pig-4.1.jar'
register '/home/virksaab/Desktop/TwitterSentimentAnalysis/pig_jars/json-simple-1.1.1.jar'

dictionary = load '/user/virksaab/AFINN-111.txt' using PigStorage('\t') AS(word:chararray,rating:int);

dump dictionary

load_tweets = LOAD '/user/flume/tweets/' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS myMap;

dump load_tweets

extract_details = FOREACH load_tweets GENERATE myMap#'id' as id,myMap#'text' as text;

tokens = foreach extract_details generate id,text, FLATTEN(TOKENIZE(text)) As word;

dump tokens

word_rating = join tokens by word left outer, dictionary by word using 'replicated';

dump word_rating

describe word_rating;

rating = foreach word_rating generate tokens::id as id,tokens::text as text, dictionary::rating as rate;

dump rating

word_group = group rating by (id,text);

avg_rate = foreach word_group generate group, AVG(rating.rate) as tweet_rating;

dump avg_rate

positive_tweets = filter avg_rate by tweet_rating>=0;

dump positive_tweets

neg = filter avg_rate by tweet_rating<0;

dump neg

store neg into "/user/virksaab/neg_tweets" using PigStorage (',')
store positive_tweets into "/user/virksaab/pos_tweets" using PigStorage (',')
