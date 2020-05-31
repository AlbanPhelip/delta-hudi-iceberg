organization := "fr.publicissapient.engineering.dataxdays"
name := "hudi"
version := "0.1.0"

scalaVersion := "2.12.11"

// Versions
val hudiVersion = "0.5.2-incubating"

// Common
libraryDependencies += "fr.publicissapient.engineering.dataxdays" %% "common" % "0.1.0"

// Apache Hudi
libraryDependencies += "org.apache.hudi" %% "hudi-spark-bundle" % hudiVersion
