//package com.project.csye7200
//
//import twitter4j._
//import twitter4j.auth.Authorization
//import twitter4j.auth.OAuthAuthorization
//import twitter4j.conf.ConfigurationBuilder
//import org.apache.spark._
//import org.apache.spark.SparkContext._
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.twitter._
//import org.apache.spark.streaming.StreamingContext._
//import org.apache.kafka.clients.producer.{KafkaProducer,ProducerRecord}
//import scala.io.Source
//import org.apache.spark.internal.Logging
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream._
//import org.apache.spark.streaming.receiver.Receiver
//
//import java.util.Properties
//
//object TweetScrapper extends App {
//
//  def logIn() = {
//    import scala.io.Source
//    for (line <- Source.fromFile("/Users/vigneshthanigaisivabalan/NEU/BD with Scala/Fake-News-Detection-with-NLP-main/src/main/scala/APITokens").getLines) {
//      val fields = line.split(" ")
//      if (fields.length == 2) {
//        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
//      }
//    }
//  }
//
//  logIn()
//
//  val consumerKey = "n9eOzCkvW5AAz6TL3PVyLYze7";
//  val consumerSecret = "LTcYrcaDZkfyoS0LB4ljFyevXOkFghHT8wUV9WgYP0ddnVJQU9";
//  val accessToken = "715063474020159488-DM5gahSh42CUyZhgyIOhZIMknHzNtl1";
//  val accessTokenSecret = "thmCipSvNC7HJVM9BUUKHiJNSOqd02FEfrSvVyvjNsqRM";
//
//  val cb = new ConfigurationBuilder
//  cb.setDebugEnabled(true)
//    .setOAuthConsumerKey(consumerKey)
//    .setOAuthConsumerSecret(consumerSecret)
//    .setOAuthAccessToken(accessToken)
//    .setOAuthAccessTokenSecret(accessTokenSecret)
//    .setJSONStoreEnabled(true)
//
//  val stream = new TwitterStreamFactory(cb.build()).getInstance()
//
//  val props = new Properties()
//  props.put("bootstrap.servers", "localhost:9092");
//  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//  val producer = new KafkaProducer[String, String](props)
//  val kafkatopic = "Tweets"
//
//  val statuslistener = new StatusListener {
//    def onStatus(status:Status) {
//      println(status.getText)
//      val data = new ProducerRecord[String, String](kafkatopic, null, status.getText)
//      producer.send(data) // (topic,key,value) //TwitterObjectFactory.getRawJSON(status)
//    }
//    def onDeletionNotice(statusDeletionNotice:StatusDeletionNotice) {
//      println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId())
//    }
//    def onScrubGeo(userId:Long, upToStatusId:Long) {
//      println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId)
//    }
//    def onStallWarning(warning:StallWarning) {
//      println("Got stall warning:" + warning)
//    }
//    def onTrackLimitationNotice(numberOfLimitedStatuses:Int) {
//      println("Got track limitation notice:" + numberOfLimitedStatuses)
//    }
//    def onException(ex:Exception) {
//      ex.printStackTrace()
//    }
//  }
//
//  stream.addListener(statuslistener)
//
//  val keywords:String = "worldnews"
//  val languages:String = "en"
//
//  val query = new FilterQuery().track(keywords).language(languages)
//  stream.filter(query)
//  Thread.sleep(5000)
//
//
//}
//
//
//
