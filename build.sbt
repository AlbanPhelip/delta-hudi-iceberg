

name := "fr.publicissapient.engineering.delta-hudi-iceberg"

version := "0.1"

scalaVersion := "2.12.11"

// Versions
val jacksonVersion = "2.7.9"
val sparkVersion = "2.4.5"
val deltaLakeVersion = "0.6.0"
val hudiVersion = "0.5.2-incubating"
val icebergVersion = "0.8.0-incubating"

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

// Spark
libraryDependencies += excludeJackson("org.apache.spark" %% "spark-core" % sparkVersion)
libraryDependencies += excludeJackson("org.apache.spark" %% "spark-sql" % sparkVersion)
libraryDependencies += "org.apache.spark" %% "spark-avro" % sparkVersion

// Delta Lake
libraryDependencies += "io.delta" %% "delta-core" % deltaLakeVersion

// Apache Hudi
libraryDependencies += "org.apache.hudi" %% "hudi-spark-bundle" % hudiVersion

// Apache Iceberg
libraryDependencies += excludeJackson("org.apache.iceberg" % "iceberg-core" % icebergVersion)
libraryDependencies += excludeJackson("org.apache.iceberg" % "iceberg-spark" % icebergVersion)
libraryDependencies += excludeJackson("org.apache.iceberg" % "iceberg-hive" % icebergVersion)

// libraryDependencies += excludeJackson("org.apache.hive" % "hive-metastore" % "3.1.2")
