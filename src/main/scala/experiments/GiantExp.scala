package experiments


import java.util.Calendar

import interlinkers.GIAnt
import model.Entity
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import utils.Constants.{GridType, Relation}
import utils.readers.Reader
import utils.{ConfigurationParser, Utils}

object GiantExp {

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

        // Parsing input arguments
        @scala.annotation.tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case ("-c" | "-conf") :: value :: tail =>
                    nextOption(map ++ Map("conf" -> value), tail)
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
                case "-gt" :: value :: tail =>
                    nextOption(map ++ Map("gt" -> value), tail)
                case "-s" :: tail =>
                    nextOption(map ++ Map("stats" -> "true"), tail)
                case _ :: tail =>
                    log.warn("DS-JEDAI: Unrecognized argument")
                    nextOption(map, tail)
            }
        }

        val argList = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), argList)

        if (!options.contains("conf")) {
            log.error("DS-JEDAI: No configuration file!")
            System.exit(1)
        }

        val confPath = options("conf")
        val conf = ConfigurationParser.parse(confPath)
        val partitions: Int = if (options.contains("partitions")) options("partitions").toInt else conf.getPartitions
        val gridType: GridType.GridType = if (options.contains("gt")) GridType.withName(options("gt").toString) else conf.getGridType
        val relation = conf.getRelation
        val printCount = options.getOrElse("stats", "false").toBoolean

        val startTime = Calendar.getInstance().getTimeInMillis

        // reading source dataset
        val reader = Reader(partitions, gridType, printCount)
        val sourceRDD: RDD[(Int, Entity)] = reader.loadSource(conf.source)
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = reader.counter

        // reading target dataset
        val targetRDD: RDD[(Int, Entity)] = reader.load(conf.target) match {
            case Left(e) =>
                log.error("Partitioner is not initialized, call first the `loadSource`.")
                e.printStackTrace()
                System.exit(1)
                null
            case Right(rdd) => rdd
        }
        val targetCount = reader.counter
        val partitioner = reader.partitioner

        Utils(sourceRDD.map(_._2.mbr), conf.getTheta, reader.partitionsZones)
        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

        val matchingStartTime = Calendar.getInstance().getTimeInMillis
        val giant = GIAnt(sourceRDD, targetRDD, partitioner)

        if (printCount){
            log.info(s"DS-JEDAI: Source geometries: $sourceCount")
            log.info(s"DS-JEDAI: Target geometries: $targetCount")
            log.info(s"DS-JEDAI: Cartesian: ${sourceCount*targetCount}")
            log.info(s"DS-JEDAI: Candidate Pairs: ${giant.countCandidates}")
        }
        else if (relation.equals(Relation.DE9IM)) {
            val (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin, verifications, qp) = giant.countAllRelations

            val totalRelations = totalContains + totalCoveredBy + totalCovers + totalCrosses + totalEquals +
                totalIntersects + totalOverlaps + totalTouches + totalWithin
            log.info("DS-JEDAI: Total Verifications: " + verifications)
            log.info("DS-JEDAI: Qualifying Pairs : " + qp)

            log.info("DS-JEDAI: CONTAINS: " + totalContains)
            log.info("DS-JEDAI: COVERED BY: " + totalCoveredBy)
            log.info("DS-JEDAI: COVERS: " + totalCovers)
            log.info("DS-JEDAI: CROSSES: " + totalCrosses)
            log.info("DS-JEDAI: EQUALS: " + totalEquals)
            log.info("DS-JEDAI: INTERSECTS: " + totalIntersects)
            log.info("DS-JEDAI: OVERLAPS: " + totalOverlaps)
            log.info("DS-JEDAI: TOUCHES: " + totalTouches)
            log.info("DS-JEDAI: WITHIN: " + totalWithin)
            log.info("DS-JEDAI: Total Discovered Relations: " + totalRelations)
        }
        else{
            val totalMatches = giant.countRelation(relation)
            log.info("DS-JEDAI: " + relation.toString +": " + totalMatches)
        }
        val matchingEndTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Interlinking Time: " + (matchingEndTime - matchingStartTime) / 1000.0)

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)
    }
}
