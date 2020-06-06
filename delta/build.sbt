organization := "fr.publicissapient.engineering.dataxdays"
name := "delta"
version := "0.1.0"

version := "0.1"

scalaVersion := "2.12.11"

// Versions
val deltaLakeVersion = "0.6.1"

// Common
libraryDependencies += "fr.publicissapient.engineering.dataxdays" %% "common" % "0.1.0"

// Delta Lake
libraryDependencies += "io.delta" %% "delta-core" % deltaLakeVersion