package experiments

import java.util
import java.util.Calendar

import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import utils.ConfigurationParser
import utils.Constants.{FileTypes, Relation}
import org.apache.spark.sql.functions.udf

object GeoSparkExp {

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val sparkConf = new SparkConf()
            .setAppName("DS-JedAI")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = new SparkContext(sparkConf)
        val spark: SparkSession = SparkSession.builder().getOrCreate()
        GeoSparkSQLRegistrator.registerAll(spark)

        // Parsing input arguments
        @scala.annotation.tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case ("-c" | "-conf") :: value :: tail =>
                    nextOption(map ++ Map("conf" -> value), tail)
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
                case _ :: tail =>
                    log.warn("DS-JEDAI: Unrecognized argument")
                    nextOption(map, tail)
            }
        }
        val startTime = Calendar.getInstance().getTimeInMillis

        val arglist = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), arglist)

        val conf_path = options("conf")
        val conf = ConfigurationParser.parse(conf_path)
        val partitions: Int = if (options.contains("partitions")) options("partitions").toInt else conf.getPartitions
        val relation = conf.getRelation

        val delimiter = conf.source.getExtension match {
            case FileTypes.CSV => ","
            case FileTypes.TSV => "\t"
            case _ => ""
        }
        val isValid = udf((g: Geometry) => g.isValid)
        val sourcePath = conf.source.path
        val source =  spark.read.format("csv")
            .option("delimiter", delimiter)
            .option("quote", "\"")
            .option("header", value = true)
            .load(sourcePath)
            .filter(col(conf.source.realIdField.get).isNotNull)
            .filter(col(conf.source.geometryField).isNotNull)
            .filter(! col(conf.source.geometryField).contains("EMPTY"))
            .filter(! col(conf.source.geometryField).contains("GEOMETRYCOLLECTION"))

        source.createOrReplaceTempView("Source")
        val sourceQuery = s"SELECT ST_GeomFromWKT(Source.${conf.source.geometryField}) AS WKT,  Source.${conf.source.realIdField.get} AS REAL_ID FROM Source".stripMargin
        val sourceDF = spark.sql(sourceQuery).withColumn("valid", isValid(col("WKT"))).filter(col("valid"))
        sourceDF.createOrReplaceTempView("sSource")

        val targetPath = conf.target.path
        val target =  spark.read.format("csv")
            .option("delimiter", delimiter)
            .option("quote", "\"")
            .option("header", value = true)
            .load(targetPath)
            .filter(col(conf.target.realIdField.get).isNotNull)
            .filter(col(conf.target.geometryField).isNotNull)
            .filter(! col(conf.target.geometryField).contains("EMPTY"))
            .filter(! col(conf.target.geometryField).contains("GEOMETRYCOLLECTION"))

        target.createOrReplaceTempView("Target")
        val targetQuery = s"SELECT ST_GeomFromWKT(Target.${conf.target.geometryField}) AS WKT,  Target.${conf.target.realIdField.get} AS REAL_ID FROM Target".stripMargin
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
        log.info(s"GEOSPARK: Total intersections: $results")

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)

    }

}
