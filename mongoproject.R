#load rmongo package
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
mongo.count(mongo, TweetData)
# > 1459855, it works

mongo.find.one(mongo, TweetData)
# _id : 7 	 5669b7cf3b8eedb1e25c8675
# id : 18 	 481723507731345408
# id_member : 16 	 235574878
# timestamp : 2 	 2014-06-25 09:00:08
# text : 2 	 Wind 7.0 kts SSE. Barometer 1018.43 mb  Steady. Temperature 14.8 A2ï¿½C. Rain today 4.0 mm. Humidity 85%
# geo_lat : 1 	 53.200000
# geo_lng : 1 	 -3.200000



# 1_ Get all the different values inside id_members
if(mongo.is.connected(mongo) == TRUE) {
  id_members <- mongo.distinct(mongo,TweetData, "id_member")
}
# > 119231

# 3_ Latest and earliest tweets

dat1 <- mongo.bson.from.JSON('{"$sort":{"timestamp":-1}}')
dat2 <- mongo.bson.from.JSON('{"$limit":1}')
maxDat <- list(dat1, dat2)
mongo.aggregation(mongo, TweetData, maxDat)

dat3 <- mongo.bson.from.JSON('{"$sort":{"timestamp":1}}')
dat4 <- mongo.bson.from.JSON('{"$limit":1}')
MinDat <- list(dat3, dat4)
mongo.aggregation(mongo, TweetData, minDat)




mongo.count(mongo, TweetData, list(id_members))

###query <- mongo.bson.from.list(list("geolat" = 53.200000))




#mongo.destroy(mongo)
