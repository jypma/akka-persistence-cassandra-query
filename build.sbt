organization := "com.tradeshift"

name := "akka-persistence-cassandra-query"

version := "0.1-201511031256"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

scalaVersion := "2.11.7"

scalacOptions ++= "-deprecation" :: "-feature" :: "-target:jvm-1.8" :: Nil

resolvers += Resolver.bintrayRepo("jypma", "maven")

libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value

libraryDependencies ++= {
  val akkaVersion = "2.4.0"
  val akkaStreamVersion = "1.0"

  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "test",
    "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
    "com.typesafe.akka" %% "akka-persistence-query-experimental" % akkaVersion,
    "com.typesafe.akka" %% "akka-http-experimental" % akkaStreamVersion
  )
}

libraryDependencies += "org.iq80.leveldb" % "leveldb" % "0.7" % "test" // only to make eclipse happy, we're not using it.

// source: https://github.com/jypma/akka-persistence-cassandra/tree/time_index_dev
libraryDependencies += "com.github.krasserm" %% "akka-persistence-cassandra" % "0.5-jypma-201511031129"

libraryDependencies += "com.datastax.cassandra"  % "cassandra-driver-core" % "2.1.8"

libraryDependencies += "org.cassandraunit" % "cassandra-unit" % "2.1.9.2" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.mockito" % "mockito-core" % "1.10.19" % "test"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.12" % "test"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

//scapegoatIgnoredFiles := Seq(".*/src_managed/main/.*")

//coverageExcludedPackages := "<empty>;Reverse.*;.*AuthService.*;models\\.data\\..*"
 
