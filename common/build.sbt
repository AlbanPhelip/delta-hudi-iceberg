organization := "fr.publicissapient.engineering.dataxdays"
name := "common"
version := "0.1.0"

scalaVersion := "2.12.11"

// Versions
val sparkVersion = "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-avro" % sparkVersion

publishTo := Some(Resolver.file("local-ivy", file("~/.ivy2/local")))