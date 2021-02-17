package experiments

import java.util.Calendar

import EntityMatching.SemiDistributedMatching.SDMFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import utils.{ConfigurationParser, SpatialReader, Utils}


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 *
 *         Execution:
 *         		spark-submit --master local[*] --class experiments.LightExp target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf <conf>
 *         Debug:
 *         		spark-submit --master local[*] --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 --class experiments.LightExp target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf <conf>
 */


object LightExp {

    def main(args: Array[String]): Unit = {
        val startTime = Calendar.getInstance().getTimeInMillis

        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val sparkConf = new SparkConf()
            .setAppName("DS-JedAI")
            .set("spark.serializer", classOf[KryoSerializer].getName)
        val sc = new SparkContext(sparkConf)
        val spark: SparkSession = SparkSession.builder().getOrCreate()

        // Parsing the input arguments
        @scala.annotation.tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case ("-c" | "-conf") :: value :: tail =>
                    nextOption(map ++ Map("conf" -> value), tail)
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
                case ("-b" | "-budget") :: value :: tail =>
                    nextOption(map ++ Map("budget" -> value), tail)
                case "-ws" :: value :: tail =>
                    nextOption(map ++ Map("ws" -> value), tail)
                case "-ma" :: value :: tail =>
                    nextOption(map ++ Map("ma" -> value), tail)
                case _ :: tail =>
                    log.warn("DS-JEDAI: Unrecognized argument")
                    nextOption(map, tail)
            }
        }

        val arglist = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), arglist)

        if (!options.contains("conf")) {
            log.error("DS-JEDAI: No configuration file!")
            System.exit(1)
        }

        val conf_path = options("conf")
        val conf = ConfigurationParser.parse(conf_path)
        val partitions: Int = if (options.contains("partitions")) options("partitions").toInt else conf.getPartitions
        val budget: Int = if (options.contains("budget")) options("budget").toInt else conf.getBudget
        val ws: String = if (options.contains("ws")) options("ws").toString else conf.getWeightingScheme.toString
        val ma: String = if (options.contains("ma")) options("ma").toString else conf.getMatchingAlgorithm.toString
        log.info("DS-JEDAI: Input Budget: " + budget)
        log.info("DS-JEDAI: Weighting Strategy: " + ws.toString)


        val reader = SpatialReader(conf.source, 20)
        val sourceRDD = reader.load().map(_._2)
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = sourceRDD.setName("SourceRDD").persist(StorageLevel.MEMORY_AND_DISK).count().toInt
        log.info("DS-JEDAI: Number of profiles of Source: " + sourceCount + " in " + sourceRDD.getNumPartitions +" partitions")


        Utils(sourceRDD.map(_.mbr), conf.getTheta, reader.partitionsZones)

        val targetRDD = reader.load(conf.target).map(_._2)
        val targetCount = targetRDD.setName("TargetRDD").persist(StorageLevel.MEMORY_AND_DISK).count().toInt
        log.info("DS-JEDAI: Number of profiles of Target: " + targetCount + " in " + targetRDD.getNumPartitions +" partitions")
        val partitioner = reader.partitioner

        val sma = SDMFactory.getMatchingAlgorithm(conf, sourceRDD, targetRDD, budget, ws, ma)
        val (totalContains, totalCoveredBy, totalCovers,totalCrosses, totalEquals, totalIntersects,
        totalOverlaps, totalTouches, totalWithin,intersectingPairs, interlinkedGeometries) = sma.countRelations

        val totalRelations = totalContains + totalCoveredBy + totalCovers + totalCrosses + totalEquals +
            totalIntersects + totalOverlaps + totalTouches + totalWithin
        log.info("DS-JEDAI: Total Intersecting Pairs: " + intersectingPairs)
        log.info("DS-JEDAI: Interlinked Geometries: " + interlinkedGeometries)

        log.info("DS-JEDAI: CONTAINS: " + totalContains)
        log.info("DS-JEDAI: COVERED BY: " + totalCoveredBy)
        log.info("DS-JEDAI: COVERS: " + totalCovers)
        log.info("DS-JEDAI: CROSSES: " + totalCrosses)
        log.info("DS-JEDAI: EQUALS: " + totalEquals)
        log.info("DS-JEDAI: INTERSECTS: " + totalIntersects)
        log.info("DS-JEDAI: OVERLAPS: " + totalOverlaps)
        log.info("DS-JEDAI: TOUCHES: " + totalTouches)
        log.info("DS-JEDAI: WITHIN: " + totalWithin)
        log.info("DS-JEDAI: Total Top Relations: " + totalRelations)


        //log.info("DS-JEDAI: Matches: " + matches.count)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }
}
