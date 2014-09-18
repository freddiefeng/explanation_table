name := "Explanation Table"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.0.0"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "0.13.1"

libraryDependencies += "net.jpountz.lz4" % "lz4" % "1.2.0"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.10.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"