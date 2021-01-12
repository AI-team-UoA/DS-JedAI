name := "DS-JedAI"
version := "0.1"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"

resolvers += "AKSW Maven Snapshots" at "https://maven.aksw.org/archiva/repository/snapshots"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
	"org.apache.spark" %%  "spark-core" % sparkVersion % Provided,
	"org.apache.spark" %%  "spark-sql" % sparkVersion  % Provided,
	"org.scalanlp" %% "breeze" % "1.0"
)

// https://mvnrepository.com/artifact/net.sansa-stack/sansa-rdf-spark
libraryDependencies += "net.sansa-stack" %% "sansa-rdf-spark" % "0.7.1"

//// https://mvnrepository.com/artifact/net.sansa-stack/sansa-query-spark
//libraryDependencies += "net.sansa-stack" %% "sansa-query-spark" % "0.7.2-SNAPSHOT"

// https://mvnrepository.com/artifact/net.sansa-stack/sansa-query-spark-sparqlify
libraryDependencies += "net.sansa-stack" %% "sansa-query-spark-sparqlify" % "0.3.0"


// https://mvnrepository.com/artifact/org.datasyslab/geospark
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"

// https://mvnrepository.com/artifact/org.datasyslab/geospark
libraryDependencies += "org.datasyslab" % "geospark-sql_2.3" % "1.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion


// https://mvnrepository.com/artifact/org.yaml/snakeyaml
libraryDependencies += "org.yaml" % "snakeyaml" % "1.8"

libraryDependencies += "net.jcazevedo" %% "moultingyaml" % "0.4.0"

// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.0"


assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}