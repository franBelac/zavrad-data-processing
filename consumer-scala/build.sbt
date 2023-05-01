ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "consumer-scala",
    idePackagePrefix := Some("org.example"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-streaming" % "3.3.2",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2",
      "org.apache.kafka" % "kafka-clients" % "3.4.0"
    )
  )
