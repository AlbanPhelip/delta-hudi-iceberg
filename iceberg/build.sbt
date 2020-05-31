organization := "fr.publicissapient.engineering.dataxdays"
name := "iceberg"
version := "0.1.0"

scalaVersion := "2.12.11"

// Versions
val icebergVersion = "0.8.0-incubating"

// Common
libraryDependencies += "fr.publicissapient.engineering.dataxdays" %% "common" % "0.1.0"

// Apache Iceberg
libraryDependencies += "org.apache.iceberg" % "iceberg-core" % icebergVersion
libraryDependencies += "org.apache.iceberg" % "iceberg-spark" % icebergVersion
libraryDependencies += "org.apache.iceberg" % "iceberg-hive" % icebergVersion

