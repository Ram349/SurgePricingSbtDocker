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
								 "org.apache.spark" %% "spark-streaming" % "2.3.0"
								 
								)

						

enablePlugins(DockerPlugin)

docker <<= (docker dependsOn assembly)

dockerfile in docker := {
  val artifact = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"
  new Dockerfile {
    from("openjdk:8-jre")
    add(artifact, artifactTargetPath)
    entryPoint("java", "-jar", artifactTargetPath)
  }
}

buildOptions in docker := BuildOptions(cache = false)

								
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

