FROM openjdk:8-jre-alpine

ADD target/scala-**/SurgePricingDemoAssembly.jar app.jar

ENTRYPOINT ["java","-jar","/app.jar"]