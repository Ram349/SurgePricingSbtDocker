//package demo.surge.pricing

//This class produces data records for PassenegerRequests which are used to create kafka messages for PassengerRequests topic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream

object PassengerRecordDataProducer {
       def main(args : Array[String]) = {
             
           val session = SparkSession
                        .builder()
                        .appName("Passenger Requests Data Producer")
                        .master("local[*]")   // Replace local with Master's URL
                        .getOrCreate(); 
           try{ 
             
                  val prop = new Properties()
                  prop.load(new FileInputStream("/home/ec2-user/Config/config.properties"))
                             
                  val sourcePath =  prop.getProperty("passenger.request.source.csv.path")
                  val destinationPath =   prop.getProperty("passenger.request.destination.csv.path")
                  val sleepTime = prop.getProperty("passenger.request.data.producer.sleep.time").toLong
                  val dataSamplingFraction = prop.getProperty("passenger.request.data.producer.sampling.fraction").toDouble
                  
                  println(" sourcePath :" + sourcePath)
                  println(" destinationPath :" + destinationPath)
                  
                  val PassengerRequests = session.read
                                                   .option("HEADER","TRUE")
                                                   .option("MODE","DROPMALFORMED")
                                                   .option("inferschema","true")
                                                   .csv(sourcePath) 
                  
                    
                 while(true){        //.sample(true, .0010)
                   PassengerRequests.sample(true, dataSamplingFraction).write.option("header", "true").mode(SaveMode.Overwrite).csv(destinationPath);
                   Thread.sleep(sleepTime);
                 }
                  
           }catch {
                // printing the cause & stack trace if exception while creating or processing the dataframe arises
                case e: org.apache.spark.sql.AnalysisException => {
                  println("\n Passenger Requests Data Producer Issue Cause : " + e.getCause)
                  println("\n Passenger Requests Data Producer Issue StackTrace : " + e.printStackTrace())
                }
                case io: java.io.IOException => {
                  //file input/output exceptions to be handled here
                  println("\n Passenger Requests Data Producer File Issue Cause : " + io.getCause)
                  println("\n Passenger Requests Data Producer File Issue StackTrace : " + io.printStackTrace())
                }
          } finally {
                // closing spark session 
                session.close()   
                println("Closed Passenger Requests Data Producer Spark Session")
          }
           
           
           
       }      
}       