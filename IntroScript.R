datab <- mtcarslibrary

#load rmongo package
library(rmongodb)

#Create mongo
mongo <- mongo.create()

#Connect to mongodb file previously created named "mongo"
mongo.is.connected(mongo)

#Get db from mongo (created there)
mongo.get.databases(mongo)

#