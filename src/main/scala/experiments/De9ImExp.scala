package experiments


import java.util.Calendar

import EntityMatching.DistributedMatching.DMFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Constants.Relation
import utils.Readers.SpatialReader
import utils.{ConfigurationParser, Utils}

object De9ImExp {

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

        // Parsing input arguments
        @scala.annotation.tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case ("-c" | "-conf") :: value :: tail =>
                    nextOption(map ++ Map("conf" -> value), tail)
                case ("-f" | "-fraction") :: value :: tail =>
                    nextOption(map ++ Map("fraction" -> value), tail)
                case ("-s" | "-stats") :: tail =>
                    nextOption(map ++ Map("stats" -> "true"), tail)
                case "-auc" :: tail =>
                    nextOption(map ++ Map("auc" -> "true"), tail)
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
        val stats = options.contains("stats")

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
        val relation = conf.getRelation

        log.info("DS-JEDAI: Input Budget: " + budget)
        log.info("DS-JEDAI: Weighting Strategy: " + ws.toString)
        val startTime = Calendar.getInstance().getTimeInMillis

        val reader = SpatialReader(conf.source, partitions)
        val sourceRDD = reader.load()
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        Utils(sourceRDD.map(_._2.mbb), conf.getTheta, reader.partitionsZones)

        val targetRDD = reader.load(conf.target)
        val partitioner = reader.partitioner

        val matchingStartTime = Calendar.getInstance().getTimeInMillis
        if (options.contains("auc")) {

            val pm = DMFactory.getProgressiveAlgorithm(conf, sourceRDD, targetRDD, partitioner, budget, ws, ma)

            val (auc, interlinkedGeometries, counter) = pm.getAUC(relation)
            log.info("DS-JEDAI: Total Intersecting Pairs: " + counter)
            log.info("DS-JEDAI: Interlinked Geometries: " + interlinkedGeometries)
            log.info("DS-JEDAI: AUC: " + auc)
        }
        else {

            val pm = DMFactory.getMatchingAlgorithm(conf, sourceRDD, targetRDD, partitioner, budget, ws, ma)

            if (relation.equals(Relation.DE9IM)) {
                val (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
                totalOverlaps, totalTouches, totalWithin, intersectingPairs, interlinkedGeometries) = pm.countAllRelations

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
                log.info("DS-JEDAI: Total Relations Discovered: " + totalRelations)
            }
            else{
                val totalMatches = pm.countRelation(relation)
                log.info("DS-JEDAI:" + relation.toString +": " + totalMatches)
            }
            val matchingEndTime = Calendar.getInstance().getTimeInMillis
            log.info("DS-JEDAI: Interlinking Time: " + (matchingEndTime - matchingStartTime) / 1000.0)
        }

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)
    }
}
