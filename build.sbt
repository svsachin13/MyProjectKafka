name := "MyProjectKafka"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.kafka" % "kafka_2.11" % "0.10.1.0","com.typesafe.akka" %% "akka-stream-experimental" % "1.0","org.specs2" %% "specs2" % "2.4.6" % "test")
    