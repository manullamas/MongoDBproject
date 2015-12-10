datab <- mtcarslibrary

#load rmongo package
library(rmongodb)

#Create mongo
mongo <- mongo.create()

#Connect to mongodb file previously created named "mongo"
mongo.is.connected(mongo)

#Get db from mongo (if created there)
mongo.get.databases(mongo)







mongDB <- mongo.bson.from.df(datab)

#Create database in mongo
mongo.insert.batch(mongo, "manu.tweets", mongDB)
