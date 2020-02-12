import Algorithms.RADON
import DataStructures.Comparison
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import utils.{BlockUtils, ConfigurationParser}
import utils.Reader.CSVReader


object Main {

	def main(args: Array[String]): Unit = {

		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)
		val log = LogManager.getRootLogger
		log.setLevel(Level.INFO)

		val sparkConf = new SparkConf()
			.setAppName("SD-JedAI")
			.set("spark.serializer",classOf[KryoSerializer].getName)
		val sc = new SparkContext(sparkConf)

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

		val sourcePath = conf.source.path
		val sourceFileExtension = sourcePath.toString.split("\\.").last
		val sourceRDD =
			sourceFileExtension match {
				case "csv" => CSVReader.loadProfiles(sourcePath, conf.source.realIdField, conf.source.geometryField)
				case _ =>
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
			}
		val sourceCount = sourceRDD.setName("SourceRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Source: " + sourceCount)

		val indexSeparator = sourceCount.toInt

		val targetPath = conf.target.path
		val targetFileExtension = targetPath.toString.split("\\.").last
		val targetRDD =
			targetFileExtension match {
				case "csv" => CSVReader.loadProfiles2(targetPath, conf.target.realIdField, conf.target.geometryField, startIdFrom=indexSeparator)
				case _ =>
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
			}

		val targetCount = targetRDD.setName("TargetRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Target: " + targetCount)


		val radon = new RADON(sourceRDD, targetRDD, conf.relation, conf.theta_measure)
		val blocks = radon.sparseSpaceTiling().persist(StorageLevel.MEMORY_AND_DISK)
		log.info("DS-JEDAI: Number of Blocks: " + blocks.count())

		val comparisons = BlockUtils.cleanBlocks2(blocks).count
		log.info("Total comparisons " + comparisons)
	}
}
