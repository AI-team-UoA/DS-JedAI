name := "DS-JedAI"
version := "0.1"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.3"



libraryDependencies ++= Seq(
	// α"net.sansa-stack" %% "sansa-rdf-spark" % "0.7.1",
	"org.apache.spark" %%  "spark-core" % sparkVersion % Provided,
	"org.apache.spark" %%  "spark-sql" % sparkVersion  % Provided,
	//"org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
	"org.scalanlp" %% "breeze" % "1.0"
)

// https://mvnrepository.com/artifact/org.datasyslab/geospark
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"

// https://mvnrepository.com/artifact/org.datasyslab/geospark
libraryDependencies += "org.datasyslab" % "geospark-sql_2.3" % "1.2.0"

// https://mvnrepository.com/artifact/com.vividsolutions/jts
libraryDependencies += "com.vividsolutions" % "jts" % "1.13"

// https://mvnrepository.com/artifact/org.yaml/snakeyaml
libraryDependencies += "org.yaml" % "snakeyaml" % "1.8"

libraryDependencies += "net.jcazevedo" %% "moultingyaml" % "0.4.0"

// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.0"


assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}