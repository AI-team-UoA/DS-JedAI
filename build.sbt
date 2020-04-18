name := "DS-JedAI"
version := "0.1"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"


// https://mvnrepository.com/artifact/org.datasyslab/geospark
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"

// https://mvnrepository.com/artifact/org.datasyslab/geospark
libraryDependencies += "org.datasyslab" % "geospark-sql_2.3" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion % Provided

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion % Provided

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