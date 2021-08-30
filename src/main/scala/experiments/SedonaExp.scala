package experiments

import java.util.Calendar

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import utils.configuration.Constants.{FileTypes, Relation}
import utils.configuration.ConfigurationParser

object SedonaExp {

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val sparkConf = new SparkConf()
            .setAppName("DS-JedAI")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
        val sc = new SparkContext(sparkConf)
        val spark: SparkSession = SparkSession.builder().getOrCreate()
       SedonaSQLRegistrator.registerAll(spark)

        // Parsing input arguments
        val parser = new ConfigurationParser()
        val configurationOpt = parser.parse(args) match {
            case Left(errors) =>
                errors.foreach(e => log.error(e.getMessage))
                System.exit(1)
                None
            case Right(configuration) => Some(configuration)
        }
        val conf = configurationOpt.get
        val partitions: Int = conf.getPartitions
        val relation = conf.getRelation
        val startTime = Calendar.getInstance().getTimeInMillis

        val delimiter = conf.source.getExtension match {
            case FileTypes.CSV => ","
            case FileTypes.TSV => "\t"
            case _ => ""
        }

        val sourceIdField = s"_c${conf.source.realIdField.get}"
        val sourceGeometryField = s"_c${conf.source.geometryField}"
        val targetIdField = s"_c${conf.target.realIdField.get}"
        val targetGeometryField = s"_c${conf.target.geometryField}"

        val isValid = udf((g: Geometry) => g.isValid)
        val sourcePath = conf.source.path
        val source =  spark.read.format("csv")
            .option("delimiter", delimiter)
            .option("quote", "\"")
            .option("header", value = false)
            .load(sourcePath)
            .filter(col(sourceIdField).isNotNull)
            .filter(col(sourceGeometryField).isNotNull)
            .filter(! col(sourceGeometryField).contains("EMPTY"))
            .filter(! col(sourceGeometryField).contains("GEOMETRYCOLLECTION"))

        source.createOrReplaceTempView("Source")
        val sourceQuery = s"SELECT ST_GeomFromWKT(Source.$sourceGeometryField) AS WKT,  Source.$sourceIdField AS REAL_ID FROM Source".stripMargin
        val sourceDF = spark.sql(sourceQuery).withColumn("valid", isValid(col("WKT"))).filter(col("valid"))
        sourceDF.createOrReplaceTempView("sSource")

        val targetPath = conf.target.path
        val target =  spark.read.format("csv")
            .option("delimiter", delimiter)
            .option("quote", "\"")
            .option("header", value = false)
            .load(targetPath)
            .filter(col(targetIdField).isNotNull)
            .filter(col(targetGeometryField).isNotNull)
            .filter(! col(targetGeometryField).contains("EMPTY"))
            .filter(! col(targetGeometryField).contains("GEOMETRYCOLLECTION"))

        target.createOrReplaceTempView("Target")
        val targetQuery = s"SELECT ST_GeomFromWKT(Target.$targetGeometryField) AS WKT,  Target.$targetIdField AS REAL_ID FROM Target".stripMargin
        val targetDF = spark.sql(targetQuery).withColumn("valid", isValid(col("WKT"))).filter(col("valid"))
        targetDF.createOrReplaceTempView("sTarget")

        val function = relation match {
            case Relation.CONTAINS => s"ST_Contains"
            case Relation.TOUCHES => s"ST_Touches"
            case Relation.WITHIN => s"ST_Within"
            case Relation.EQUALS => s"ST_Equals"
            case Relation.CROSSES => s"ST_Crosses"
            case Relation.OVERLAPS => s"ST_Overlaps"
            case _ => s"ST_Intersects"
        }
        val spatialQuery = s"SELECT sSource.REAL_ID, sTarget.REAL_ID FROM sSource, sTarget WHERE $function(sSource.WKT, sTarget.WKT)"
        val results = spark.sql(spatialQuery).count()
        log.info(s"DS-JEDAI: Total intersections: $results")

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)

    }

}
