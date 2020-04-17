package experiments

import java.util.Calendar

import Blocking.{BlockUtils, BlockingFactory}
import EntityMatching.Matching
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import utils.Reader.CSVReader
import utils.Utils.printPartitions
import utils.{ConfigurationParser, Constants, SpatialPartitioner, Utils}

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
		val partitions: Int = conf.configurations.getOrElse(Constants.CONF_PARTITIONS, "-1").toInt
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
		val sourceCount = sourceRDD.setName("SourceRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Source: " + sourceCount)
		val indexSeparator = sourceCount.toInt

		// Loading Target
		val targetPath = conf.target.path
		val targetFileExtension = targetPath.toString.split("\\.").last
		val targetRDD =
			targetFileExtension match {
				case "csv" =>
					val data = CSVReader.loadProfiles2(targetPath, conf.target.realIdField, conf.target.geometryField, startIdFrom=indexSeparator)
					if (repartition && !spatialPartition)
						data.repartition(partitions)
					else data
				case _ =>
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
			}
		val targetCount = targetRDD.setName("TargetRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Target: " + targetCount)

		// Swapping: set the set with the smallest area as source
		val (source, target, relation) = {
			if (spatialPartition) {
				// swapping
				val (s, t, r) = Utils.swappingStrategy(sourceRDD, targetRDD, conf.relation)

				// Spatial partitioning
				val sPartitioning_startTime = Calendar.getInstance()
				var (spatialPartitionedSource, spatialPartitionedTarget) =
					if (repartition) SpatialPartitioner.spatialPartitionT(s, t, partitions=partitions)
					else SpatialPartitioner.spatialPartitionT(s, t)

				// caching and un-persisting previous RDDs
				spatialPartitionedSource = spatialPartitionedSource.setName("SpatialPartitionedSource").persist(StorageLevel.MEMORY_AND_DISK)
				spatialPartitionedTarget = spatialPartitionedTarget.setName("SpatialPartitionedTarget").persist(StorageLevel.MEMORY_AND_DISK)
				sourceRDD.unpersist()
				targetRDD.unpersist()

				val sPartitioning_time = (Calendar.getInstance().getTimeInMillis - sPartitioning_startTime.getTimeInMillis)/ 1000.0
				log.info("DS-JEDAI: Spatial Partitioning Took: " + sPartitioning_time )
				log.info("DS-JEDAI: Source Partition Distribution")
				printPartitions(spatialPartitionedSource.asInstanceOf[RDD[Any]])
				log.info("DS-JEDAI: Target Partition Distribution")
				printPartitions(spatialPartitionedTarget.asInstanceOf[RDD[Any]])

				(spatialPartitionedSource, spatialPartitionedTarget, r)
			}
			else
				Utils.swappingStrategy(sourceRDD, targetRDD, conf.relation)
		}

		if (conf.relation == Constants.DISJOINT) {
			val matching_startTime = Calendar.getInstance().getTimeInMillis
			val matches = Matching.disjointMatches(source, target).setName("Matches").persist(StorageLevel.MEMORY_AND_DISK)
			log.info("DS-JEDAI: Matches: " + matches.count)
			val matching_endTime = Calendar.getInstance().getTimeInMillis
			log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)
		}
		else {

			// Blocking
			val blocking_startTime = Calendar.getInstance().getTimeInMillis
			val blocking = BlockingFactory.getBlocking(conf, source, target)
			val blocks = blocking.apply().setName("Blocks").persist(StorageLevel.MEMORY_AND_DISK)
			val totalBlocks = blocks.count()
			log.info("DS-JEDAI: Number of Blocks: " + totalBlocks)
			val blocking_endTime = Calendar.getInstance().getTimeInMillis
			log.info("DS-JEDAI: Blocking Time: " + (blocking_endTime - blocking_startTime) / 1000.0)
			BlockUtils.setTotalBlocks(totalBlocks)

			// Entity Matching
			val matching_startTime = Calendar.getInstance()
			//val matches = Matching.prioritizedMatching(blocks, relation)
			val matches = Matching.SpatialMatching(blocks, relation)
			log.info("DS-JEDAI: Matches: " + matches.count)
			val matching_endTime = Calendar.getInstance()
			log.info("DS-JEDAI: Matching Time: " + (matching_endTime.getTimeInMillis - matching_startTime.getTimeInMillis) / 1000.0)

			val endTime = Calendar.getInstance()
			log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime.getTimeInMillis) / 1000.0)
		}
		//System.in.read
		//spark.stop()
	}
}
