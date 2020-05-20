package experiments

import java.util.Calendar

import Blocking.BlockingFactory
import EntityMatching.DistributedAlgorithms.{DistributedMatchingFactory, SpatialMatching}
import EntityMatching.DistributedAlgorithms.prioritization.{BlockCentricPrioritization, ComparisonCentricPrioritization}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import utils.Readers.Reader
import utils.{ConfigurationParser, Constants, Utils}

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 *
 *         Execution:
 *         		spark-submit --master local[*] --class experiments.SpatialExp target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf <conf>
 *         Debug:
 *         		spark-submit --master local[*] --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 --class experiments.SpatialExp target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf <conf>
 */

object SpatialExp {

	def main(args: Array[String]): Unit = {
		val startTime =  Calendar.getInstance().getTimeInMillis

		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)
		val log = LogManager.getRootLogger
		log.setLevel(Level.INFO)

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

		// TODO configuration arguments must return from configuration
		val conf_path = options("conf")
		val conf = ConfigurationParser.parse(conf_path)
		val partitions: Int = conf.configurations.getOrElse(Constants.CONF_PARTITIONS, "-1").toInt
		val repartition: Boolean = partitions > 0
		val spatialPartition: Boolean = conf.configurations.getOrElse(Constants.CONF_SPATIAL_PARTITION, "false").toBoolean

		// Loading Source
		val sourceRDD = Reader.read(conf.source.path, conf.source.realIdField, conf.source.geometryField, spatialPartition)
		val sourceCount = sourceRDD.setName("SourceRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Source: " + sourceCount)
		val indexSeparator = sourceCount.toInt

		// Loading Target
		val targetRDD = Reader.read(conf.target.path, conf.source.realIdField, conf.source.geometryField, spatialPartition, indexSeparator)
		val targetCount = targetRDD.setName("TargetRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Target: " + targetCount)

		val (source, target, relation) = Utils.swappingStrategy(sourceRDD, targetRDD, conf.relation)


		if (conf.relation == Constants.DISJOINT) {
			val matching_startTime = Calendar.getInstance().getTimeInMillis
			val matcher = SpatialMatching(-1)
			val matches = matcher.disjointMatches(source, target).setName("Matches").persist(StorageLevel.MEMORY_AND_DISK)
			log.info("DS-JEDAI: Matches: " + matches.count)
			val matching_endTime = Calendar.getInstance().getTimeInMillis
			log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)
		}
		else {

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
			val matches = DistributedMatchingFactory.getMatchingAlgorithm(conf, totalBlocks).apply(blocks, relation, Constants.RANDOM)
			//val matches = SpatialMatching.SpatialMatching(blocks, relation)
			log.info("DS-JEDAI: Matches: " + matches.count)
			val matching_endTime = Calendar.getInstance().getTimeInMillis
			log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)

			val endTime = Calendar.getInstance()
			log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
		}
		//System.in.read
		//spark.stop()
	}
}
