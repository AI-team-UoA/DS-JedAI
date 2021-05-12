package experiments

import java.util.Calendar

import interlinkers.{GIAnt, IndexedJoinInterlinking}
import model.TileGranularities
import model.entities.Entity
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.locationtech.jts.geom.Geometry
import utils.Constants.{GridType, Relation}
import utils.configurationParser.ConfigurationParser
import utils.readers.{GridPartitioner, Reader}


object BalancingExp {

    implicit class TupleAdd(t: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)) {
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
            .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
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
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
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
        val gridType: GridType.GridType = if (options.contains("gt")) GridType.withName(options("gt").toString) else conf.getGridType
        val relation = conf.getRelation
        val startTime = Calendar.getInstance().getTimeInMillis

        // load datasets
        val sourceSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.source)
        val targetSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.target)

        // spatial partition
        val partitioner = GridPartitioner(sourceSpatialRDD, partitions, gridType)
        val sourceRDD: RDD[(Int, Entity)] = partitioner.transform(sourceSpatialRDD, conf.source)
        val targetRDD: RDD[(Int, Entity)] = partitioner.transform(targetSpatialRDD, conf.target)
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)

        val theta = TileGranularities(sourceRDD.map(_._2.env))
        val partitionBorder = partitioner.getAdjustedBordersOfMBR(theta)
        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

        val sourcePartitions: RDD[(Int, Iterator[Entity])] = sourceRDD.mapPartitions(si => Iterator((TaskContext.getPartitionId(), si.map(_._2))))
        val entitiesPerPartitions: Seq[(Int, Int)] =  sourcePartitions.map{ case (pid, si) => (pid, si.size)}.collect()

        // find outlier partitions
        val mean = entitiesPerPartitions.map(_._2).sum/sourceRDD.getNumPartitions
        val variance =  entitiesPerPartitions.map(_._2.toDouble).map(x => math.pow(x - mean, 2)).sum / entitiesPerPartitions.length
        val std = Math.sqrt(variance)
        val zScore: (Int, Int) => (Int, Double) = (p: Int, x: Int) => (p, (x - mean).toDouble/std)

        val outliers = entitiesPerPartitions.map{case (p, x) => zScore(p, x)}.filter(_._2 > 2.5)
        val outlierPartitions = outliers.map(_._1).toSet
        log.info("DS-JEDAI: Overloaded partitions: " + outlierPartitions.size)

        val goodSourceRDD = sourceRDD.filter(s => !outlierPartitions.contains(s._1))
        val badSourceRDD = sourceRDD.filter(s => outlierPartitions.contains(s._1))

        val goodTargetRDD = targetRDD.filter(t => !outlierPartitions.contains(t._1))
        val badTargetRDD = targetRDD.filter(t => outlierPartitions.contains(t._1))

        val giant = GIAnt(goodSourceRDD, goodTargetRDD, theta, partitionBorder, partitioner.hashPartitioner)
        val iji = IndexedJoinInterlinking(badSourceRDD, badTargetRDD, theta, partitionBorder)

        if (relation.equals(Relation.DE9IM)) {
            val giantStartTime = Calendar.getInstance().getTimeInMillis
            val giantResults = giant.countAllRelations
            val giantEndTime = Calendar.getInstance().getTimeInMillis
            log.info("DS-JEDAI: GIA.nt Time: " + (giantEndTime - giantStartTime) / 1000.0)
            log.info("DS-JEDAI: GIA.nt Interlinked Geometries: " + giantResults._11)
            log.info("-----------------------------------------------------------\n")

            val indexedJoinStartTime = Calendar.getInstance().getTimeInMillis
            val indexedJoinResults = iji.countAllRelations
            val indexedJoinEndTime = Calendar.getInstance().getTimeInMillis
            log.info("DS-JEDAI: INDEXED-JOIN Time: " + (indexedJoinEndTime - indexedJoinStartTime) / 1000.0)
            log.info("DS-JEDAI:INDEXED-JOIN Interlinked Geometries: " + indexedJoinResults._11)
            log.info("-----------------------------------------------------------\n")

            val (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin, intersectingPairs, interlinkedGeometries) = giantResults + indexedJoinResults
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
            val totalMatches = giant.countRelation(relation) + iji.countRelation(relation)
            log.info("DS-JEDAI: " + relation.toString +": " + totalMatches)
        }

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }

}
