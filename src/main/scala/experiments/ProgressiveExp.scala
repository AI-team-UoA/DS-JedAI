package experiments

import java.util.Calendar

import interlinkers.progressive.ProgressiveAlgorithmsFactory
import model.Entity
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.Geometry
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.{GridType, ProgressiveAlgorithm, Relation, WeightingFunction}
import utils.Constants.WeightingFunction.WeightingFunction
import utils.configurationParser.ConfigurationParser
import utils.readers.{GridPartitioner, Reader}
import utils.{Constants, Utils}

object ProgressiveExp {

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
                case ("-b" | "-budget") :: value :: tail =>
                    nextOption(map ++ Map("budget" -> value), tail)
                case "-mwf" :: value :: tail =>
                    nextOption(map ++ Map("mwf" -> value), tail)
                case "-swf" :: value :: tail =>
                    nextOption(map ++ Map("swf" -> value), tail)
                case "-pa" :: value :: tail =>
                    nextOption(map ++ Map("pa" -> value), tail)
                case "-gt" :: value :: tail =>
                    nextOption(map ++ Map("gt" -> value), tail)
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
                case "-ws" :: value :: tail =>
                    nextOption(map ++ Map("ws" -> value), tail)
                case "-time" :: tail =>
                    nextOption(map ++ Map("time" -> "true"), tail)
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

        // extract arguments
        val confPath = options("conf")
        val conf = ConfigurationParser.parse(confPath)
        val partitions: Int = if (options.contains("partitions")) options("partitions").toInt else conf.getPartitions
        val gridType: GridType.GridType = if (options.contains("gt")) GridType.withName(options("gt").toString) else conf.getGridType
        val budget: Int = if (options.contains("budget")) options("budget").toInt else conf.getBudget
        val mainWF: WeightingFunction = if (options.contains("mwf")) WeightingFunction.withName(options("mwf")) else conf.getMainWF
        val secondaryWF: Option[WeightingFunction] = if (options.contains("swf")) Option(WeightingFunction.withName(options("swf"))) else conf.getSecondaryWF
        val ws: Constants.WeightingScheme = if (options.contains("ws")) utils.Constants.WeightingSchemeFactory(options("ws")) else conf.getWS
        val pa: ProgressiveAlgorithm = if (options.contains("pa")) ProgressiveAlgorithm.withName(options("pa")) else conf.getProgressiveAlgorithm
        val timeExp: Boolean = options.contains("time")
        val relation = conf.getRelation

        log.info(s"DS-JEDAI: Weighting Scheme: ${ws.value}")
        log.info(s"DS-JEDAI: Input Budget: $budget")
        log.info(s"DS-JEDAI: Main Weighting Function: ${mainWF.toString}")
        if (secondaryWF.isDefined) log.info(s"DS-JEDAI: Secondary Weighting Function: ${secondaryWF.get.toString}")
        log.info(s"DS-JEDAI: Progressive Algorithm: ${pa.toString}")

        val startTime = Calendar.getInstance().getTimeInMillis

        // load datasets
        val sourceSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.source)
        val targetSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.target)

        // spatial partition
        val partitioner = GridPartitioner(sourceSpatialRDD, partitions, gridType)
        val sourceRDD: RDD[(Int, Entity)] = partitioner.distribute(sourceSpatialRDD, conf.source)
        val targetRDD: RDD[(Int, Entity)] = partitioner.distribute(targetSpatialRDD, conf.target)
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = sourceRDD.count()

        val theta = Utils.getTheta(sourceRDD.map(_._2.mbr))
        val partitionBorder = Utils.getBordersOfMBR(partitioner.partitionBorders, theta).toArray
        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

        val matchingStartTime = Calendar.getInstance().getTimeInMillis
        val method = ProgressiveAlgorithmsFactory.get(pa, sourceRDD, targetRDD, theta, partitionBorder,
            partitioner.hashPartitioner, sourceCount, budget, mainWF, secondaryWF, ws)

        if(timeExp){
            //invoke load of target
            targetRDD.count()

            val times = method.time
            val schedulingTime = times._1
            val verificationTime = times._2
            val matchingTime = times._3

            log.info(s"DS-JEDAI: Scheduling time: $schedulingTime")
            log.info(s"DS-JEDAI: Verification time: $verificationTime")
            log.info(s"DS-JEDAI: Interlinking Time: $matchingTime")
        }

        else if (relation.equals(Relation.DE9IM)) {
            val (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin, verifications, qp) = method.countAllRelations

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
            log.info("DS-JEDAI: Total Relations Discovered: " + totalRelations)
        }

        else{
            val totalMatches = method.countRelation(relation)
            log.info("DS-JEDAI: " + relation.toString +": " + totalMatches)
        }

        val matchingEndTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Interlinking Time: " + (matchingEndTime - matchingStartTime) / 1000.0)

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)
    }

}
