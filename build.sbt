name := "snowflake-insert-performance"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.26",
  "net.snowflake" % "snowflake-jdbc" % "3.12.14",
  "org.apache.commons" % "commons-csv" % "1.8",
  "org.rogach" %% "scallop" % "3.5.1"
)
