name := "Surge Pricing Demo"
version := "1.0"
organization := "ram.pradhan.demo"

scalaVersion := "2.11.8"
	
	libraryDependencies ++= Seq(
								"org.apache.spark" % "spark-core_2.11" % "2.3.0",
								"org.apache.spark" % "spark-sql_2.11" % "2.3.0",
								 "org.apache.kafka" %% "kafka" % "2.1.0",
								 "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.1.0",
								 "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0",
								 "org.apache.spark" %% "spark-streaming" % "2.3.0",
								 "com.spotify" % "docker-client" % "3.5.13"
								)

jarName in assembly := "SurgePricingDemoAssembly.jar"							
								
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

