package experiments

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import utils.readers.Reader

import java.util.Calendar
import scala.util.Random

/**
 * @author George Mandilaras (NKUA)
 */
object GeometryTransformer {

    val rng: Random = new Random()
    rng.setSeed(Calendar.getInstance().getTimeInMillis)
    val leftLimit = 0.5f
    val rightLimit = 1.5f
    val geometryFactory = new GeometryFactory()


    def parseCommandLineArguments(args: Seq[String]): Map[String, String] ={
        @scala.annotation.tailrec
        def nextOption(map: Map[String, String], list: List[String]): Map[String, String] = {
            list match {
                case Nil => map
                case ("-dt" | "-dataset") :: value :: tail =>
                    nextOption(map ++ Map("dataset" -> value), tail)
                case ("-t" | "-times") :: value :: tail =>
                    nextOption(map ++ Map("times" -> value), tail)
            }
        }
        val argList = args.toList
        nextOption(Map(), argList)
    }


    def geometryTransformation(geometry: Geometry): Geometry ={

        def transformCoordinates(coordinates: Array[Coordinate]): Array[Coordinate] ={
            coordinates.map{c =>
                val shift = leftLimit + (rng.nextInt() * (rightLimit - leftLimit))
                val (x, y) = rng.nextInt(3) match {
                    case 0 => (c.x+shift, c.y+shift)
                    case 1 => (c.x-shift, c.y-shift)
                    case 2 => (c.x*shift, c.y*shift)
                    case _ => (c.x, c.y)
                }
                new Coordinate(x, y)
            }
        }
        val coordinates = geometry.getCoordinates
        geometry.getGeometryType match {
            case Geometry.TYPENAME_LINESTRING => geometryFactory.createLineString(transformCoordinates(coordinates))
            case Geometry.TYPENAME_POINT => geometryFactory.createPoint(transformCoordinates(coordinates).head)
            case Geometry.TYPENAME_POLYGON => geometryFactory.createPolygon(transformCoordinates(coordinates))
            case _ => geometry
        }
    }

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


        val optionsMap = parseCommandLineArguments(args)
        val path = optionsMap("dataset")
        val times = optionsMap.getOrElse("times", "1").toInt
        val sRDD: SpatialRDD[Geometry] = Reader.readFile(path, Some("1"), "0", None)
        (0 to times).foreach(i =>
            sRDD.rawSpatialRDD.rdd.zipWithUniqueId().map{ case (geometry, index) =>
                s"${geometryTransformation(geometry).toText}\t$i$index"
            }
                .saveAsTextFile(s"${path}_$i")
        )
    }
}
