organization := "fr.publicissapient.engineering.dataxdays"
name := "iceberg"
version := "0.1.0"

scalaVersion := "2.12.11"

// Versions
val icebergVersion = "0.8.0-incubating"
val jacksonVersion = "2.7.9"

// Jackson
def excludeJackson(dependency: ModuleID): ModuleID = dependency excludeAll(
  ExclusionRule(organization = "com.fasterxml.jackson.module"),
  ExclusionRule(organization = "com.fasterxml.jackson.core")
)

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-paranamer" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % jacksonVersion

// Common
libraryDependencies += excludeJackson("fr.publicissapient.engineering.dataxdays" %% "common" % "0.1.0")

// Apache Iceberg
libraryDependencies += excludeJackson("org.apache.iceberg" % "iceberg-core" % icebergVersion)
libraryDependencies += excludeJackson("org.apache.iceberg" % "iceberg-spark" % icebergVersion)
libraryDependencies += excludeJackson("org.apache.iceberg" % "iceberg-hive" % icebergVersion)

