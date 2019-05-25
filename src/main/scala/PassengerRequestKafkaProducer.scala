//package demo.surge.pricing

//This class produces messages to be published to kafka server under topic "PassengerRequests"

import java.util.Properties._
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.FileNotFoundException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SQLContext._
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.ProducerRecord._
import java.util.Properties
import java.io.FileInputStream

object PassengerRequestKafkaProducer {
      def main(args : Array[String]) = {
  
      val session = SparkSession
                    .builder()
                    .appName("Passenger Requests Kafka Producer")
                    .master("local[*]")   // Replace local with Master's URL
                    .getOrCreate();    
 
      try{ 
              val ConfigProperties = new Properties()
              ConfigProperties.load(new FileInputStream("/home/ec2-user/Config/config.properties"))
              val kafkaBootstrapServers =  ConfigProperties.getProperty("passenger.request.kafka.producer.bootstrap.servers")
              val sourcePath =   ConfigProperties.getProperty("passenger.request.kafka.producer.source.csv.path")
              val sleepTime = ConfigProperties.getProperty("passenger.request.kafka.producer.sleep.time").toLong
                  
              val KafkaProperties = new Properties()
              KafkaProperties.put("bootstrap.servers", kafkaBootstrapServers)
              KafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
              KafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
              //-- KafkaProperties.put("value.serializer", "PassengerRequestSerializer")
              
              import session.implicits._     
              
              while(true){
                    val PassengerRequestsDS = session
                                          .read
                                          .option("mode","DROPMALFORMED")
                                          .option("header","true")
                                          .option("inferschema","true")
                                          .csv(sourcePath)
                                          .withColumn("RequestID", 'RequestID.cast(LongType))
                                          .withColumn("Latitude", 'Latitude. cast(DoubleType))
                                          .withColumn("Longitude", 'Longitude.cast(DoubleType))
                                          .withColumn("RequestDate", 'RequestDate.cast(StringType))
                                          .as[PassengerRequest] 
                                   
                    //val EachPassengerRequestTuple =  DriverLocationsDS.filter( DriverLocationsDS("Latitude").isNotNull && DriverLocationsDS("Longitude").isNotNull)     
                    
                    PassengerRequestsDS.foreachPartition((partitions: Iterator[PassengerRequest]) => {
                      //-- val producer = new KafkaProducer[String, PassengerRequest](KafkaProperties) 
                      val producer = new KafkaProducer[String, String](KafkaProperties)
                      partitions.foreach((EachPassengerRequest: PassengerRequest) => {
                        try {
                          var Topic = GeoHash.encode(EachPassengerRequest.Latitude ,EachPassengerRequest.Longitude,6)
                          //-- var record = new ProducerRecord("PassengerRequests", Topic,EachPassengerRequest.asInstanceOf[PassengerRequest])
                           var record = new ProducerRecord("PassengerRequests", Topic,EachPassengerRequest.toString())
                          producer.send(record)
                        } catch {
                          case ex: Exception => {
                            printf(ex.getMessage, ex)
                          }
                        }
                      })
                      producer.close()
                    })
                    
             Thread.sleep(sleepTime);
             }        
              
       }catch {
                // printing the cause & stack trace if exception while creating or processing the dataframe arises
                case e: org.apache.spark.sql.AnalysisException => {
                  println("\n Passenger Request Kafka Producer Issue Cause : " + e.getCause)
                  println("\n Passenger Request Kafka Producer Issue StackTrace : " + e.printStackTrace())
                }
                case io: java.io.IOException => {
                  //file input/output exceptions to be handled here
                  println("\n Passenger Request Kafka Producer File Issue Cause : " + io.getCause)
                  println("\n Passenger Request Kafka Producer File Issue StackTrace : " + io.printStackTrace())
                }
          } finally {
                // closing spark session 
                session.close()   
                println("Closed Passenger Request Kafka Producer Spark Session")
          }    

  }
  
}