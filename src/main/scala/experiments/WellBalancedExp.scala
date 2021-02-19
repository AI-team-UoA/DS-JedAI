package experiments

import java.util.Calendar

import geospatialInterlinking.IndexBasedMatching
import geospatialInterlinking.IndexBasedMatching
import geospatialInterlinking.progressive.ProgressiveAlgorithmsFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.{GridType, ProgressiveAlgorithm, Relation, WeightingScheme}
import utils.Constants.WeightingScheme.WeightingScheme
import utils.{ConfigurationParser, SpatialReader, Utils}


object WellBalancedExp {

    implicit class TuppleAdd(t: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)) {
        def +(p: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int))
        : (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
            (p._1 + t._1, p._2 + t._2, p._3 +t._3, p._4+t._4, p._5+t._5, p._6+t._6, p._7+t._7, p._8+t._8, p._9+t._9, p._10+t._10, p._11+t._11)
    }

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

        // Parsing the input arguments
        @scala.annotation.tailrec
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
                case "-gt" :: value :: tail =>
                    nextOption(map ++ Map("gt" -> value), tail)
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
        val budget: Int = if (options.contains("budget")) options("budget").toInt else conf.getBudget
        val ws: WeightingScheme = if (options.contains("ws")) WeightingScheme.withName(options("ws")) else conf.getWeightingScheme
        val ma: ProgressiveAlgorithm = if (options.contains("ma")) ProgressiveAlgorithm.withName(options("ma")) else conf.getProgressiveAlgorithm
        val gridType: GridType.GridType = if (options.contains("gt")) GridType.withName(options("gt").toString) else conf.getGridType
        val relation = conf.getRelation

        log.info("DS-JEDAI: Input Budget: " + budget)
        log.info("DS-JEDAI: Weighting Scheme: " + ws.toString)

        val startTime = Calendar.getInstance().getTimeInMillis
        val reader = SpatialReader(conf.source, partitions, gridType)
        val sourceRDD = reader.load()
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        Utils(sourceRDD.map(_._2.mbr), conf.getTheta, reader.partitionsZones)

        val targetRDD = reader.load(conf.target)
        val partitioner = reader.partitioner

        val partitionEntitiesAVG = sourceRDD.mapPartitions(si => Iterator(si.toArray.length)).sum()/sourceRDD.getNumPartitions
        val balancedSource = sourceRDD.mapPartitions(si => Iterator(si.toArray)).filter(_.length < partitionEntitiesAVG*3).flatMap(_.toIterator)
        val overloadedSource = sourceRDD.mapPartitions(si => Iterator(si.toArray)).filter(_.length >= partitionEntitiesAVG*3).flatMap(_.toIterator)
        val overloadedPartitionIds = overloadedSource.map(_ => TaskContext.getPartitionId()).collect().toSet
        val balancedTarget = targetRDD.mapPartitions(ti => Iterator((TaskContext.getPartitionId(), ti))).filter{ case (pid, _) => !overloadedPartitionIds.contains(pid) }.flatMap(_._2)
        val overloadedTarget = targetRDD.mapPartitions(ti => Iterator((TaskContext.getPartitionId(), ti))).filter{ case (pid, _) => overloadedPartitionIds.contains(pid) }.flatMap(_._2)
        log.info("DS-JEDAI: Overloaded partitions: " + overloadedPartitionIds.size)

        val matchingStartTime = Calendar.getInstance().getTimeInMillis

        val pm = ProgressiveAlgorithmsFactory.get(ma, sourceRDD, targetRDD, partitioner, budget, ws)
        val ibm = IndexBasedMatching(overloadedSource.map(_._2), overloadedTarget.map(_._2), Utils.getTheta)

        if (relation.equals(Relation.DE9IM)) {
            val (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin, intersectingPairs, interlinkedGeometries) = pm.countAllRelations + ibm.countAllRelations

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
            val totalMatches = pm.countRelation(relation) + ibm.countRelation(relation)
            log.info("DS-JEDAI: " + relation.toString +": " + totalMatches)
        }
        val matchingEndTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Interlinking Time: " + (matchingEndTime - matchingStartTime) / 1000.0)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }

}
