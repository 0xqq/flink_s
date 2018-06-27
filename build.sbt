name := "flink_s"

version := "0.1"

scalaVersion := "2.11.12"

lazy val slf4jVer = "1.7.25"
lazy val flinkVer = "1.5.0"
lazy val kafkaVer = "0.11.0.2"


libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-core" % flinkVer,
  "org.apache.flink" %% "flink-clients" % flinkVer,
  "org.apache.flink" % "flink-shaded-jackson" % "2.7.9-3.0",
  "org.apache.flink" %% "flink-table" % flinkVer,
  "org.apache.flink" %% "flink-scala" % flinkVer,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVer,
  "org.apache.flink" %% "flink-connector-kafk-0.11" % flinkVer,
  "org.apache.flink" %% "flink-connector-elasticsearch-base" % flinkVer,
  "org.apache.bahir" %% "flink-connector-redis" % "1.0",
  "org.apache.flink" %% "flink-test-utils" % flinkVer % Test,
  "org.apache.flink" %% "flink-statebackend-rocksdb" % flinkVer % Test,

  "org.apache.kafka" % "kafka-clients" % kafkaVer

).map(_.exclude("org.slf4j", "*")).map(_.exclude("com.google.guava", "*"))
  .map(_.exclude("com.fasterxml.jackson.module", "*"))
  .map(_.exclude("net.jpountz.lz4", "lz4"))

//others
libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "16.0.1",
  "org.codehaus.jettison" % "jettison" % "1.3.8",
  "net.jpountz.lz4" % "lz4" % "1.3.0",

  "commons-codec" % "commons-codec" % "1.10",
  "commons-io" % "commons-io" % "2.5",
  "pers.chenqian" % "utils_java" % "3.8.5",

  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.5",

  "mysql" % "mysql-connector-java" % "5.1.43"

).map(_.exclude("org.slf4j", "*"))


libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % slf4jVer,
  "org.slf4j" % "slf4j-api" % slf4jVer,
  "org.slf4j" % "slf4j-simple" % slf4jVer,
  "uk.org.lidalia" % "jul-to-slf4j-config" % "1.0.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.1"
)