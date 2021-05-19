name := "DS-JedAI"
version := "0.1"
scalaVersion := "2.12.12"
val sparkVersion = "2.4.3"




libraryDependencies ++= Seq(
	"org.apache.spark" %%  "spark-core" % sparkVersion % Provided,
	"org.apache.spark" %%  "spark-sql" % sparkVersion  % Provided,
	"org.apache.spark" %% "spark-graphx" % sparkVersion % Provided
)

// https://mvnrepository.com/artifact/org.apache.sedona/sedona-core-2.4
libraryDependencies += "org.apache.sedona" %% "sedona-core-2.4" % "1.0.0-incubating"

// https://mvnrepository.com/artifact/org.apache.sedona/sedona-sql-2.4
libraryDependencies += "org.apache.sedona" %% "sedona-sql-2.4" % "1.0.0-incubating"

// https://mvnrepository.com/artifact/org.locationtech.jts/jts-core
libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.18.0"

// https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper
libraryDependencies += "org.datasyslab" % "geotools-wrapper" % "geotools-24.0"

// https://mvnrepository.com/artifact/org.wololo/jts2geojson
libraryDependencies += "org.wololo" % "jts2geojson" % "0.14.3"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"

// https://mvnrepository.com/artifact/org.yaml/snakeyaml
libraryDependencies += "org.yaml" % "snakeyaml" % "1.8"

libraryDependencies += "net.jcazevedo" %% "moultingyaml" % "0.4.0"

// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.0"

// https://mvnrepository.com/artifact/org.typelevel/cats-core
libraryDependencies += "org.typelevel" %% "cats-core" % "2.6.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % Test


assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}



// SANSA STACK
//resolvers += "AKSW Maven Snapshots" at "https://maven.aksw.org/archiva/repository/snapshots"
//resolvers += "jitpack" at "https://jitpack.io"
//// https://mvnrepository.com/artifact/net.sansa-stack/sansa-rdf-spark
//libraryDependencies += "net.sansa-stack" %% "sansa-rdf-spark" % "0.7.1" excludeAll(
//	ExclusionRule("org.springframework"),
//	ExclusionRule("org.apache.hadoop"),
//	ExclusionRule("org.apache.spark"),
//	ExclusionRule("org.scala-lang"),
//	ExclusionRule("org.scalatest"),
//	ExclusionRule("it.unimi.dsi"),
//)
//
//// https://mvnrepository.com/artifact/net.sansa-stack/sansa-query-spark
//libraryDependencies += "net.sansa-stack" %% "sansa-query-spark" % "0.7.1"  excludeAll(
//	ExclusionRule("com.ibm.sparktc.sparkbench", "sparkbench"),
//	ExclusionRule("net.sansa-stack"),//, "query-tests"),
//	ExclusionRule("net.sansa-stack", "sansa-datalake-spark"),
//	ExclusionRule("net.sansa-stack", "sansa-rdf-common"),
//	ExclusionRule("org.springframework"),
//	ExclusionRule("org.apache.hadoop"),
//	ExclusionRule("org.apache.spark"),
//	ExclusionRule("io.github.litmus-benchmark-suite"),
//	ExclusionRule("org.scala-lang"),
//	ExclusionRule("org.scalatest"),
//	ExclusionRule("it.unibz.inf.ontop"),
//	ExclusionRule("it.unimi.dsi"),
//	ExclusionRule("org.aksw.jena-sparql-api"),
//	ExclusionRule("it.unimi.dsi"),
//	ExclusionRule("om.sun.xml.bind"),
//)
