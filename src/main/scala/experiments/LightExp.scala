package experiments

import java.util.Calendar

import Blocking.{BlockUtils, BlockingFactory}
import DataStructures.LightBlock
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import utils.Reader.CSVReader
import utils.{ConfigurationParser, Constants, Utils}


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
            .setAppName("SD-JedAI")
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
        val partitions: Int = conf.configurations.getOrElse(Constants.CONF_PARTITIONS, "0").toInt
        val repartition: Boolean = partitions > 0
        val spatialPartition: Boolean = conf.configurations.getOrElse(Constants.CONF_SPATIAL_PARTITION, "false").toBoolean

        // Loading Source
        val sourcePath = conf.source.path
        val sourceFileExtension = sourcePath.toString.split("\\.").last
        val sourceRDD =
            sourceFileExtension match {
                case "csv" =>
                    val data = CSVReader.loadProfiles(sourcePath, conf.source.realIdField, conf.source.geometryField)
                    if (repartition && !spatialPartition)
                        data.repartition(partitions)
                    else data
                case _ =>
                    log.error("DS-JEDAI: This filetype is not supported yet")
                    System.exit(1)
                    null
            }
        var sourceCount = sourceRDD.setName("SourceRDD").cache().count()
        log.info("DS-JEDAI: Number of ptofiles of Source: " + sourceCount)
        val indexSeparator = sourceCount.toInt

        // Loading Target
        val targetPath = conf.target.path
        val targetFileExtension = targetPath.toString.split("\\.").last
        val targetRDD =
            targetFileExtension match {
                case "csv" =>
                    val data = CSVReader.loadProfiles2(targetPath, conf.target.realIdField, conf.target.geometryField, startIdFrom = indexSeparator)
                    if (repartition && !spatialPartition)
                        data.repartition(partitions)
                    else data
                case _ =>
                    log.error("DS-JEDAI: This filetype is not supported yet")
                    System.exit(1)
                    null
            }
        var targetCount = targetRDD.setName("TargetRDD").cache().count()
        log.info("DS-JEDAI: Number of ptofiles of Target: " + targetCount)

        // Swapping: set the set with the smallest area as source
        val (source, target, relation) = Utils.swappingStrategy(sourceRDD, targetRDD, conf.relation)

        val (sourceIDs_startsFrom, targetIDs_startsFrom) = if (Utils.swapped) (indexSeparator, 0) else (0, indexSeparator)
        if (Utils.swapped) {
            val temp = sourceCount
            sourceCount = targetCount
            targetCount = temp
        }

        val liTarget = sourceCount > targetCount
        val blocking_startTime = Calendar.getInstance().getTimeInMillis
        val blocks: RDD[LightBlock] = BlockingFactory.getBlocking(conf, source, target).apply(liTarget)
            .setName("LightBlocks").persist(StorageLevel.MEMORY_AND_DISK)
        val totalBlocks = blocks.count()
        log.info("DS-JEDAI: Number of Blocks: " + totalBlocks)
        val blocking_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Blocking Time: " + (blocking_endTime - blocking_startTime) / 1000.0)

        val matching_startTime = Calendar.getInstance().getTimeInMillis
        val matches =
            if (liTarget)
                EntityMatching.Matching.lightMatching(blocks, target, targetIDs_startsFrom, relation, Utils.swapped)
            else
                EntityMatching.Matching.lightMatching(blocks, source, sourceIDs_startsFrom, relation, Utils.swapped)

        log.info("DS-JEDAI: Matches: " + matches.count)
        val matching_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)

//        System.in.read
//        spark.stop()
    }
}
