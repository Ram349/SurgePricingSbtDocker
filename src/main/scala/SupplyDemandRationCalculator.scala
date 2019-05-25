//package demo.surge.pricing

/*
This class creates SparkStram to fetch data from Kafka server for the topics "DriverLocations" & "PassengerRequests"
and does the calculation of no. of PassengerRequest & no.f Drivers present in a GeoHash Location 
and puts this data in a file in format : (Geohash,no. of Drivers,no. of PassengerRequests)
*/

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.spark.streaming.kafka010.PreferConsistent
import java.util.Properties
import java.io.FileInputStream
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime

object SupplyDemandRationCalculator {

      def main(args : Array[String]) = {
              val conf = new SparkConf()
                                  .setAppName("Supply Demand Ration Calculator")
                                  .setMaster("local[*]")
                                  .set("spark.streaming.kafka.maxRatePerPartition", "100")
            
              val sc = new SparkContext(conf)
              val sqlContext = new SQLContext(sc)
              val streamingContext = new StreamingContext(sc, Seconds(50))
             //val sparkSession = new SparkSession()
              
              try{ 
                    
                  val ConfigProperties = new Properties()
                  ConfigProperties.load(new FileInputStream("/home/ec2-user/Config/config.properties"))
                  val kafkaBootstrapServers =  ConfigProperties.getProperty("passenger.request.kafka.producer.bootstrap.servers")
                  val SupplyDemandRationOutputFile = ConfigProperties.getProperty("supply.demand.ratio.output.file.path")               
                  val kafkaParams = Map[String, Object](
                                        "bootstrap.servers" -> kafkaBootstrapServers,
                                        "key.deserializer" -> classOf[StringDeserializer],
                                        "value.deserializer" -> classOf[StringDeserializer],
                                        "group.id" -> "kafka_demo_group",
                                        "auto.offset.reset" -> "earliest",
                                        "enable.auto.commit" -> (true: java.lang.Boolean)
                    )
                  
                    /*
                    val props = new Properties()
                    props.put("bootstrap.servers", "localhost:9092")
                    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")                
                    val KafkaConsumer = new KafkaConsumer[String, String](props)
                    */
                    
                    //val topicOnServer : Array[String] = KafkaConsumer.listTopics().keySet().toArray()
                    
                    //println("List of Topics from Server: ")
                    //topicOnServer.keySet().toArray().foreach(println)
                    
                   // set the list of the GeoHash. Drivers whose location is inside this list of GeoHashes will be fetched from
                    //kafka topic for these geohashes
                  
                   
                   val topics = Array("DriverLocations","PassengerRequests")//Array("DriverLocations","PassengerRequests")
                   val stream = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent,Subscribe[String, String](topics, kafkaParams)
                    )
                    
                   //println("stream: "+stream.print())
                   
                    /*    
                  //stream.filter()  
                  println("stream: "+stream.print())  
                  val TopicKeyMap = stream.map(record => ((record.topic(),record.key()), record.value()))   //record.value
                   println("TopicKeyMap: \n" + TopicKeyMap.print())
                  println("Topic Group By Key: ")
                    TopicKeyMap.groupByKey().print
                    TopicKeyMap.reduceByKey( (x,y) => x + y).print()
                    //TopicKeyMap.reduceByKey( (x,y) => x + y )
                    //TopicKe
                   //print()
                    TopicKeyMap.groupByKey().countByValue()
                  //TopicKeyMap.foreach(print())
                    */
                   //val TopicKeyMap = stream.map(record => (record.key(),record.topic()))
                   // TopicKeyMap.reduceByKey( (x,y) => x + y).print(100)
                    
                    //TopicKeyMap.reduceByKey()
                    //key = Geohash Topic = DriverLocation/PassengerRequest
                    val GeohashTopicMap = stream.map(record => (record.key(),record.topic()))
                    val GeohashTopicMapGrouppedByGeohash =  GeohashTopicMap.groupByKey()
                    GeohashTopicMapGrouppedByGeohash.print(100)
                    println("GeohashTopicMapGrouppedByGeohash : "+ GeohashTopicMapGrouppedByGeohash.print(100))
                    
                 
                   val SupplyDemandRatioPerGeohash =  GeohashTopicMapGrouppedByGeohash.map(GeohashTopicMapGrouppedByGeohashRec => 
                         ( GeohashTopicMapGrouppedByGeohashRec._1
                          ,
                          (GeohashTopicMapGrouppedByGeohashRec._2.count(value => value == "DriverLocations")) 
                            ,
                            (GeohashTopicMapGrouppedByGeohashRec._2.count(value => value == "PassengerRequests"))
                            //, LocalDateTime.now()                    
                         )
                          
                      )
                     
                      println("SupplyDemandRatioPerGeohash: "+ SupplyDemandRatioPerGeohash.print(100))
                     SupplyDemandRatioPerGeohash.foreachRDD(
                                                            geoHashSupplyDemandMap => 
                                                                  if (geoHashSupplyDemandMap.count() == 0) {
                                                                    println("No logs received in this time interval")
                                                                  } else {
                                                                    //geoHashSupplyDemandMap.asInstanceOf[GeohashSupplyDemand].
                                                                    
                                                                    sqlContext.createDataFrame(geoHashSupplyDemandMap).toDF().write.mode("append").parquet(SupplyDemandRationOutputFile)
                                                                    
                                                                    //.saveAsTextFile("..//New//Output//GeoHashSupplyDemandRation.csv")
                                                                  }       
                                                            )
                                               
                    streamingContext.start()
                    streamingContext.awaitTermination()
                    println("stream: "+stream.print())
               }catch {
                      // printing the cause & stack trace if exception while creating or processing the dataframe arises
                      case e: org.apache.spark.sql.AnalysisException => {
                        println("\n Supply Demand Ration Calculator Issue Cause : " + e.getCause)
                        println("\n Supply Demand Ration Calculator Issue StackTrace : " + e.printStackTrace())
                      }
                      case io: java.io.IOException => {
                        //file input/output exceptions to be handled here
                        println("\n Supply Demand Ration Calculator File Issue Cause : " + io.getCause)
                        println("\n Supply Demand Ration Calculator File Issue StackTrace : " + io.printStackTrace())
                      }
                }        
       }
}