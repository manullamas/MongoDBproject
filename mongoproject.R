
tweetsdataset <- read.csv("microblogDataset_COMP6235_CW2.csv")
summary(tweetsdataset)

############# Data cleaning

# In the id_member seems to be negative values:
summary(tweetsdataset$id_member)
hist(tweetsdataset$id_member)
# To clean this I change directly the sign (plotting a histogram we can see that most of the values are positive)
tweetsdataset$id_member <- ifelse(tweetsdataset$id_member > 0, tweetsdataset$id_member, tweetsdataset$id_member * -1) 
# To check that the cleaning was correct whe can check the number of different users before and after the cleaning, that is the same: 119231.



# In the id column there is many N/A data and when counting the raw data imported in mongo in a preanalysis we can see that the number is less than the length of the dataset, apart from that the number of digits in the id is in general 18, so it is a good point of start to check the ones that are not like that
idNot18 <- tweetsdataset[(nchar(as.character(tweetsdataset$id)) != 18),]
# Many of the values are empty (no text), so to remove the bias of these data, we are going to erase them

clean1 <- tweetsdataset[(nchar(as.character(tweetsdataset$id)) == 18),]
# counting the total number of tweets it is very similar, so these data I have removed seems to be not readed by the system and good thing to remove them

# Using summary again it seems to be ids with characters, it is necessary to delete them
clean2 <- clean1[grep("[A-z]", tweetsdataset$id, invert=TRUE),]

# After this preprocessing we save the dataset again to upload to mongo and start doing some queries
write.csv(clean2, "tweetsdatasetCleant.csv")



################ queries

#load rmongo package to work on mongo through R
library(rmongodb)

#Create mongo
mongo <- mongo.create()

#Connect to mongodb file previously created named "mongo"
mongo.is.connected(mongo)

#Get db from mongo (if created there)
if(mongo.is.connected(mongo) == TRUE) {
  mongo.get.databases(mongo)
}

# Insert csv data.frame in R
#tweetsdataset <- read.csv("microblogDataset_COMP6235_CW2.csv")
# what is tweetsdataset?
#class(tweetsdataset)
#Convert to mongo.bson
#tweetsBSON = mongo.bson.from.list(tweetsdataset)
#mongo.insert.batch(mongo, "manu.tweets", tweetsBSON)
# I tried to upload the dataset from 

# Should return the collections inside "manu" database: "tweets"mongo.bson
mongo.get.database.collections(mongo, "manu")
# > Character(0). But the collection is there! (I can work with it) 

# Counting the entries in the dataset
mongo.count(mongo, "manu.tweets")
# > 1459855 (This is not the number of entries in the csv file, there is a difference of 674. Then checking the ID variable I saw that there are 674 N/A values, so mongo.count is only counting entries in the 1st column and set values with N/A like they dont exists)

# Rename the collection "tweets" that belongs to the database "manu"
TweetData <- "manu.tweets"

mongo.find.one(mongo, TweetData)
# _id : 7 	 5669b7cf3b8eedb1e25c8675
# id : 18 	 481723507731345408
# id_member : 16 	 235574878
# timestamp : 2 	 2014-06-25 09:00:08
# text : 2 	 Wind 7.0 kts SSE. Barometer 1018.43 mb  Steady. Temperature 14.8 A2ï¿½C. Rain today 4.0 mm. Humidity 85%
# geo_lat : 1 	 53.200000
# geo_lng : 1 	 -3.200000


###################################################################################

# 1_ Get all the different values inside id_members

id_members <- mongo.distinct(mongo,TweetData, "id_member")
length(id_members)

# > 119231


###################################################################################

# 2_ Percentage of tweets of the top10 users

totalTweets <- mongo.count(mongo, TweetData)
cond1 <- mongo.bson.from.JSON('{"$group":{"_id":"$id_member", "nTweets":{"$sum":1}}}')
cond2 <- mongo.bson.from.JSON('{"$sort":{"nTweets":-1}}')
cond3 <- mongo.bson.from.JSON('{"$limit":10}')
cond4 <- mongo.bson.from.JSON('{"$group" : { "_id" : null, "top10" : { "$sum": "$nTweets"}}}')
builtQuery <- mongo.aggregation(mongo, TweetData, list(cond1, cond2, cond3, cond4))
Rquery <- mongo.bson.to.Robject(builtQuery)
(Rquery$result$`0`$top10 / totalTweets)*100

#


###################################################################################

# 3_ Latest and earliest tweets

dat1 = mongo.bson.from.JSON('{"$sort":{"timestamp":-1}}')
dat2 = mongo.bson.from.JSON('{"$limit":1}')
maxDat = list(dat1, dat2)
latestTweet = mongo.aggregation(mongo, TweetData, maxDat)
# waitedMS : 18 	 0
# result : 4 	 
# 0 : 3 	 
# _id : 7 	 5669b81d3b8eedb1e26beb1c
# id : 18 	 483731699332022272
# id_member : 16 	 29227733
# timestamp : 2 	 2014-06-30 21:59:59
# text : 2 	 @Teambeatsallday @Drake Just Hold On were going home fts @50cent (rich remix) FREE DL http://t.co/rBeduVDKbB #RT
# geo_lat : 1 	 53.106812
# geo_lng : 1 	 -2.439672
# 
# 
# ok : 1 	 1.000000
latestTweetR = mongo.bson.to.list(latestTweet)$result[[1]]$timestamp
# "2014-06-30 21:59:59"


dat3 = mongo.bson.from.JSON('{"$sort":{"timestamp":1}}')
dat4 = mongo.bson.from.JSON('{"$limit":1}')
minDat = list(dat3, dat4)
earliestTweet = mongo.aggregation(mongo, TweetData, minDat)
mongo.distinct(mongo,earliestTweet, "timestamp")
# waitedMS : 18 	 0
# result : 4 	 
# 0 : 3 	 
# _id : 7 	 5669b8133b8eedb1e26a2bff
# id : 18 	 480847701492645888
# id_member : 16 	 495413413
# timestamp : 2 	 2014-06-22 23:00:00
# text : 2 	 @NiamhyFoxy happy birthday gorgeous girlie hope you have a great day!ðŸŽ‰ðŸŽŠðŸŽðŸŽˆâœŒï¸ xx
# geo_lat : 1 	 51.430984
# geo_lng : 1 	 -2.844654
# 
# 
# ok : 1 	 1.000000
earliestTweetR = mongo.bson.to.list(earliestTweet)$result[[1]]$timestamp
# "2014-06-22 23:00:00"


###################################################################################

# 4_ mean time delta (mathematics explaining this in the report)

LateT = (date = as.POSIXct(latestTweetR, tz='UTC'))
EarlyT = (date = as.POSIXct(earliestTweetR, tz='UTC'))
timeDiff = as.numeric(LateT-EarlyT, units="secs")/((mongo.count(mongo, "manu.tweets"))-1)
# 0.4710034

###################################################################################

# 5_ mean length of a message

# try to make the cursor iterate only through the text column
library(stringr)
sum = 0
cursor = mongo.find(mongo,TweetData)
counter = 0
while (mongo.cursor.next(cursor)) {
  counter = counter + 1
  print(counter)
  # iterate and grab the next record
  tmp = mongo.bson.to.list(mongo.cursor.value(cursor))
  sum = sum + str_length(tmp$text)
}
print(sum)
avgLength = sum/mongo.count(mongo,TweetData)
# 72.54083


###################################################################################

# 6_


###################################################################################

# 7_ Average number of hashtags in a message
# library(ngram)
# library(stringr)
# sum = 0
# cursor = mongo.find(mongo,TweetData)
# counter = 0
# while (mongo.cursor.next(cursor)) {
#   counter = counter + 1
#   print(counter)


###################################################################################






mongo.destroy(mongo)
