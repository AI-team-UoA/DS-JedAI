package experiments

import java.util.Calendar

import interlinkers.progressive.ProgressiveAlgorithmsFactory
import model.Entity
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.WeightingFunction.WeightingFunction
import utils.Constants.{GridType, ProgressiveAlgorithm, WeightingFunction}
import utils.readers.Reader
import utils.{ConfigurationParser, Constants, Utils}

object TimeExp {

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
        val mainWF: WeightingFunction = if (options.contains("mwf")) WeightingFunction.withName(options("mwf")) else conf.getMainWF
        val secondaryWF: Option[WeightingFunction] = if (options.contains("swf")) Option(WeightingFunction.withName(options("swf"))) else conf.getSecondaryWF
        val pa: ProgressiveAlgorithm = if (options.contains("pa")) ProgressiveAlgorithm.withName(options("pa")) else conf.getProgressiveAlgorithm
        val gridType: GridType.GridType = if (options.contains("gt")) GridType.withName(options("gt").toString) else conf.getGridType
        val ws: Constants.WeightingScheme = if (options.contains("ws")) utils.Constants.WeightingSchemeFactory(options("ws")) else conf.getWS


        log.info("DS-JEDAI: Input Budget: " + budget)
        log.info("DS-JEDAI: Main Weighting Function: " + mainWF.toString)
        if (secondaryWF.isDefined) log.info("DS-JEDAI: Secondary Weighting Function: " + secondaryWF.get.toString)
        log.info("DS-JEDAI: Progressive Algorithm: " + pa.toString)

        val startTime = Calendar.getInstance().getTimeInMillis / 1000d

        val reader = Reader(partitions, gridType)
        val sourceRDD: RDD[(Int, Entity)] = reader.loadSource(conf.source)
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)

        val targetRDD: RDD[(Int, Entity)] = reader.load(conf.target) match {
            case Left(e) =>
                log.error("Partitioner is not initialized, call first the `loadSource`.")
                e.printStackTrace()
                System.exit(1)
                null
            case Right(rdd) => rdd
        }
        val partitioner = reader.partitioner

        Utils(sourceRDD.map(_._2.mbr), conf.getTheta, reader.partitionsZones)
        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

        targetRDD.count()
        val method = ProgressiveAlgorithmsFactory.get(pa, sourceRDD, targetRDD, partitioner, budget, mainWF, secondaryWF, ws)

        val times = method.time
        val schedulingTime = times._1
        val verificationTime = times._2
        val matchingTime = times._3

        log.info(s"DS-JEDAI: Scheduling time: $schedulingTime")
        log.info(s"DS-JEDAI: Verification time: $verificationTime")
        log.info(s"DS-JEDAI: Interlinking Time: $matchingTime")

        val endTime = Calendar.getInstance().getTimeInMillis / 1000d
        val totalTime = endTime - startTime
        log.info(s"DS-JEDAI: Total Execution Time: $totalTime")
    }

}
