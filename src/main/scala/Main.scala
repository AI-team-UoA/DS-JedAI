import Blocking.RADON
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import utils.ConfigurationParser
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

		// TODO: Check if YAML is valid
		val conf_path = options("conf")
		val conf = ConfigurationParser.parse(conf_path)

		val sourcePath = conf.source.path
		val sourceFileExtension = sourcePath.toString.split("\\.").last
		val sourceRDD =
			sourceFileExtension match {
				case "csv" => CSVReader.loadProfiles(sourcePath, conf.source.realIdField, conf.source.geometryField)
				case _ => {
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
				}
			}
		val sourceCount = sourceRDD.setName("SourceRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Source: " + sourceCount)

		val targetPath = conf.target.path
		val targetFileExtension = targetPath.toString.split("\\.").last
		val targetRDD =
			targetFileExtension match {
				case "csv" => CSVReader.loadProfiles(targetPath, conf.target.realIdField, conf.target.geometryField)
				case _ => {
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
				}
			}

		val targetCount = targetRDD.setName("TargetRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Target: " + targetCount)

		val sourceETH = RADON.getETH(sourceRDD, sourceCount)
		val targetETH = RADON.getETH(targetRDD, targetCount)
	}
}
