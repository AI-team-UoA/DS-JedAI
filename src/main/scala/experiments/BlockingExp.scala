package experiments

import java.util.Calendar

import Blocking.BlockingFactory
import EntityMatching.BlockBasedAlgorithms.BlockMatchingFactory
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
 *         		spark-submit --master local[*] --class experiments.SpatialExp target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf <conf>
 *         Debug:
 *         		spark-submit --master local[*] --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 --class experiments.SpatialExp target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf <conf>
 */

object BlockingExp {

	def main(args: Array[String]): Unit = {
		val startTime =  Calendar.getInstance().getTimeInMillis

		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)
		val log = LogManager.getRootLogger
		log.setLevel(Level.ERROR)

		val sparkConf = new SparkConf()
			.setAppName("SD-JedAI")
			.set("spark.serializer",classOf[KryoSerializer].getName)
		val sc = new SparkContext(sparkConf)
		val spark: SparkSession = SparkSession.builder().getOrCreate()

		// Parsing the input arguments
		@scala.annotation.tailrec
		def nextOption(map: OptionMap, list: List[String]): OptionMap = {
			list match {
				case Nil => map
				case ("-c" |"-conf") :: value :: tail =>
					nextOption(map ++ Map("conf" -> value), tail)
				case _ :: tail=>
					log.warn("DS-JEDAI: Unrecognized argument")
					nextOption(map, tail)
			}
		}

		val arglist = args.toList
		type OptionMap = Map[String, String]
		val options = nextOption(Map(), arglist)

		if(!options.contains("conf")){
			log.error("DS-JEDAI: No configuration file!")
			System.exit(1)
		}

		val conf_path = options("conf")
		val conf = ConfigurationParser.parse(conf_path)
		val spatialPartition: Boolean = conf.getSpatialPartitioning

		// Loading Source
		val reader = SpatialReader(conf.source, 20)
		val sourceRDD = reader.load().map(_._2)
		sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
		val sourceCount = sourceRDD.setName("SourceRDD").persist(StorageLevel.MEMORY_AND_DISK).count().toInt
		log.info("DS-JEDAI: Number of profiles of Source: " + sourceCount + " in " + sourceRDD.getNumPartitions +" partitions")


		Utils(sourceRDD.map(_.mbb), conf.getTheta, reader.partitionsZones)

		val targetRDD = reader.load(conf.target).map(_._2)
		val targetCount = targetRDD.setName("TargetRDD").persist(StorageLevel.MEMORY_AND_DISK).count().toInt
		log.info("DS-JEDAI: Number of profiles of Target: " + targetCount + " in " + targetRDD.getNumPartitions +" partitions")
		val partitioner = reader.partitioner

		val (source, target, relation) = (sourceRDD, targetRDD, conf.getRelation)
		val dimensions = (sourceCount, targetCount)

		// Blocking
		val blocking_startTime = Calendar.getInstance().getTimeInMillis
		val blocking = BlockingFactory.getBlocking(conf, source, target, spatialPartition)
		val blocks = blocking.apply().setName("Blocks").persist(StorageLevel.MEMORY_AND_DISK)
		val totalBlocks = blocks.count()
		log.info("DS-JEDAI: Number of Blocks: " + totalBlocks)
		val blocking_endTime = Calendar.getInstance().getTimeInMillis
		log.info("DS-JEDAI: Blocking Time: " + (blocking_endTime - blocking_startTime) / 1000.0)


		// Entity Matching
		val matching_startTime = Calendar.getInstance().getTimeInMillis
		val matches = BlockMatchingFactory.getMatchingAlgorithm(conf, blocks, dimensions, totalBlocks).apply(relation)
		log.info("DS-JEDAI: Matches: " + matches.count)
		val matching_endTime = Calendar.getInstance().getTimeInMillis
		log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)

		val endTime = Calendar.getInstance()
		log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
	}
}
