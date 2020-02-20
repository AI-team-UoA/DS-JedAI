import java.util.Calendar

import Blocking.{BlockUtils, RADON, StaticBlocking}
import EntityMatching.Matching
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import utils.{ConfigurationParser, Constants}
import utils.Reader.CSVReader
import utils.Utils.printPartitions


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

object Main {

	def main(args: Array[String]): Unit = {
		val startTime =  Calendar.getInstance()

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

		val conf_path = options("conf")
		val conf = ConfigurationParser.parse(conf_path)

		// Loading Source
		val sourcePath = conf.source.path
		val sourceFileExtension = sourcePath.toString.split("\\.").last
		val sourceRDD =
			sourceFileExtension match {
				case "csv" => CSVReader.loadProfiles(sourcePath, conf.source.realIdField, conf.source.geometryField)
					.map(es => (es.id, es)).partitionBy(new org.apache.spark.HashPartitioner(8)).map(_._2)
				case _ =>
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
			}
		val sourceCount = sourceRDD.setName("SourceRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Source: " + sourceCount)
		val indexSeparator = sourceCount.toInt

		// Loading Target
		val targetPath = conf.target.path
		val targetFileExtension = targetPath.toString.split("\\.").last
		val targetRDD =
			targetFileExtension match {
				case "csv" => CSVReader.loadProfiles2(targetPath, conf.target.realIdField, conf.target.geometryField, startIdFrom=indexSeparator)
					.map(es => (es.id, es)).partitionBy(new org.apache.spark.HashPartitioner(8)).map(_._2)
				case _ =>
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
			}
		val targetCount = targetRDD.setName("TargetRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Target: " + targetCount)

/*
		// Spatial partitioning
		val spartitioning_startTime =  Calendar.getInstance()
		//val (spatialPartitionedSource, spatialPartitionedTarget) = Utils.spatialPartition(source, target)
		val spartitioning_endTime = Calendar.getInstance()
		log.info("DS-JEDAI: Spatial Partitioning Took: " + (spartitioning_endTime.getTimeInMillis - spartitioning_startTime.getTimeInMillis)/ 1000.0)

		log.info("DS-JEDAI: Source Partition Distribution")
		printPartitions(source.asInstanceOf[RDD[Any]])
		log.info("DS-JEDAI: Target Partition Distribution")
		printPartitions(target.asInstanceOf[RDD[Any]])
*/
		if (conf.relation == Constants.DISJOINT) {
			val matching_startTime = Calendar.getInstance()
			val matches = Matching.disjointMatches(sourceRDD, targetRDD).setName("Matches").persist(StorageLevel.MEMORY_AND_DISK)
			log.info("DS-JEDAI: Matches: " + matches.count)
			val matching_endTime = Calendar.getInstance()
			log.info("DS-JEDAI: Matching Time: " + (matching_endTime.getTimeInMillis - matching_startTime.getTimeInMillis) / 1000.0)
		}
		else {

			// Swapping: set the set with the smallest area as source
			val (source, target, relation) = BlockUtils.swappingStrategy(sourceRDD, targetRDD, conf.relation)

			// Blocking
			val blocking_startTime = Calendar.getInstance()
			//val blockingAlg = StaticBlocking(source, spatialPartitionedTarget, 10, 0.1)
			val blockingAlg = RADON(source, target, conf.theta_measure)
			val blocks = blockingAlg.apply().persist(StorageLevel.MEMORY_AND_DISK)
			log.info("DS-JEDAI: Number of Blocks: " + blocks.count())


			// Block cleaning
			val allowedComparisons = BlockUtils.cleanBlocks(blocks).setName("Comparisons").persist(StorageLevel.MEMORY_AND_DISK)
			log.info("DS-JEDAI: Comparisons Partition Distribution")
			printPartitions(allowedComparisons.asInstanceOf[RDD[Any]])
			log.info("Total comparisons " + allowedComparisons.map(_._2.length).sum().toInt)
			val blocking_endTime = Calendar.getInstance()
			log.info("DS-JEDAI: Blocking Time: " + (blocking_endTime.getTimeInMillis - blocking_startTime.getTimeInMillis) / 1000.0)

			// Entity Matching
			val matching_startTime = Calendar.getInstance()
			val matches = Matching.SpatialMatching(blocks, allowedComparisons, relation).setName("Matches").persist(StorageLevel.MEMORY_AND_DISK)
			log.info("DS-JEDAI: Matches: " + matches.count)
			val matching_endTime = Calendar.getInstance()
			log.info("DS-JEDAI: Matching Time: " + (matching_endTime.getTimeInMillis - matching_startTime.getTimeInMillis) / 1000.0)

			val endTime = Calendar.getInstance()
			log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime.getTimeInMillis) / 1000.0)
		}
	}
}
