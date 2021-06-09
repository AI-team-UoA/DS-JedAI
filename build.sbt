name := "DS-JedAI"
version := "0.1"
scalaVersion := "2.11.12"
val sparkVersion = "2.4.4"

scalacOptions ++= Seq("-feature", "-language:reflectiveCalls")

val currentDirectory = new java.io.File(".").getCanonicalPath
libraryDependencies += "org.locationtech.jts" % "jts" % "1.18.2-SNAPSHOT" from s"file://$currentDirectory/jts/modules/core/target/jts-core-1.18.2-SNAPSHOT.jar"

libraryDependencies += "ai.di.uoa.gr" % "interlinking" % "1.18.2-SNAPSHOT" from s"file://$currentDirectory/jts/interlinking/target/interlinking-1.18.2-SNAPSHOT.jar"

libraryDependencies ++= Seq(
	"org.apache.spark" %%  "spark-core" % sparkVersion % Provided,
	"org.apache.spark" %%  "spark-sql" % sparkVersion  % Provided
)

// https://mvnrepository.com/artifact/org.apache.sedona/sedona-core-2.4
libraryDependencies += "org.apache.sedona" %% "sedona-core-2.4" % "1.0.0-incubating"

// https://mvnrepository.com/artifact/org.apache.sedona/sedona-sql-2.4
libraryDependencies += "org.apache.sedona" %% "sedona-sql-2.4" % "1.0.0-incubating"

// https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper
libraryDependencies += "org.datasyslab" % "geotools-wrapper" % "geotools-24.0"

// https://mvnrepository.com/artifact/org.wololo/jts2geojson
libraryDependencies += "org.wololo" % "jts2geojson" % "0.14.3"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"

libraryDependencies += "net.jcazevedo" %% "moultingyaml" % "0.4.0"

// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.0"

// https://mvnrepository.com/artifact/org.typelevel/cats-core
libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0-RC3"


// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % Test



assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}

