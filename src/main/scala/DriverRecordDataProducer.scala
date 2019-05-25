//package demo.surge.pricing

//This class produces data records for DriverLocation which are used to create kafka messages for DriverLocations topic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream

object DriverRecordDataProducer {
       def main(args : Array[String]) = {
             
           val session = SparkSession
                        .builder()
                        .appName("Driver Records Data Producer")
                        .master("local[*]")   // Replace local with Master's URL
                        .getOrCreate(); 
           try{ 
             
                  val prop = new Properties()
                  prop.load(new FileInputStream("/home/ec2-user/Config/config.properties"))
                             
                  val sourcePath =  prop.getProperty("driver.locations.source.csv.path")
                  val destinationPath =   prop.getProperty("driver.locations.destination.csv.path")
                  val sleepTime = prop.getProperty("driver.location.data.producer.sleep.time").toLong
                  val dataSamplingFraction = prop.getProperty("driver.location.data.producer.sampling.fraction").toDouble
                  
                  println(" sourcePath :" + sourcePath)
                  println(" destinationPath :" + destinationPath)
                  
                  val DriverLocations = session.read
                                                   .option("HEADER","TRUE")
                                                   .option("MODE","DROPMALFORMED")
                                                   .option("inferschema","true")
                                                   .csv(sourcePath) 
                                      
                 while(true){                   
                   DriverLocations.sample(true, dataSamplingFraction).write.option("header", "true").mode(SaveMode.Overwrite).csv(destinationPath);
                   Thread.sleep(sleepTime);
                 }
                  
           }catch {
                // printing the cause & stack trace if exception while creating or processing the dataframe arises
                case e: org.apache.spark.sql.AnalysisException => {
                  println("\n Driver Locations Data Producer Issue Cause : " + e.getCause)
                  println("\n Driver Locations Data Producer Issue StackTrace : " + e.printStackTrace())
                }
                case io: java.io.IOException => {
                  //file input/output exceptions to be handled here
                  println("\n Driver Locations Data Producer File Issue Cause : " + io.getCause)
                  println("\n Driver Locations Data Producer File Issue StackTrace : " + io.printStackTrace())
                }
          } finally {
                // closing spark session 
                session.close()   
                println("Closed Driver Locations Data Producer Spark Session")
          }
           
           
           
       }      
}       