organization := "com.tradeshift"

name := "akka-persistence-cassandra-query"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions ++= "-deprecation" :: "-feature" :: "-target:jvm-1.8" :: Nil

libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value

libraryDependencies ++= {
  val akkaVersion = "2.4.0"
  val akkaStreamVersion = "1.0"

  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence-query-experimental" % akkaVersion,
    "com.typesafe.akka" %% "akka-http-experimental" % akkaStreamVersion
  )
}

libraryDependencies += "org.iq80.leveldb" % "leveldb" % "0.7" % "test" // only to make eclipse happy, we're not using it.

// source: https://github.com/jypma/akka-persistence-cassandra/tree/time_index
// moved to /lib for now, until fork is either merged, or built to maven central under different artifact name
// libraryDependencies += "com.github.krasserm" %% "akka-persistence-cassandra" % "0.5-SNAPSHOT"

libraryDependencies += "com.datastax.cassandra"  % "cassandra-driver-core" % "2.1.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.mockito" % "mockito-core" % "1.10.19" % "test"

//scapegoatIgnoredFiles := Seq(".*/src_managed/main/.*")

//coverageExcludedPackages := "<empty>;Reverse.*;.*AuthService.*;models\\.data\\..*"
 
