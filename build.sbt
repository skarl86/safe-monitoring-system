name := "safe-monitoring-system"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.0"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
