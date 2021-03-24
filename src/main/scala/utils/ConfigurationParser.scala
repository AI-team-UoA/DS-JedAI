package utils


import net.jcazevedo.moultingyaml.{DefaultYamlProtocol, _}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.joda.time.format.DateTimeFormat
import utils.Constants.GridType.GridType
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.Relation.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Constants.FileTypes.FileTypes
import utils.Constants._


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class DatasetConfigurations(path: String, geometryField: String, realIdField: Option[String] = None, dateField: Option[String] = None, datePattern: Option[String] = None){


	def getExtension: FileTypes = FileTypes.withName(path.toString.split("\\.").last)

	def check():Boolean ={
		val log: Logger = LogManager.getRootLogger

		// checks path and geometry filed
		if (geometryField == ""){
			log.error(s"DS-JEDAI: Path: \'$path\' does not exist")
			false
		}
		else {
			// a date is valid iff both field and pattern are defined and the pattern is valid
			val dateCheck =
				if (dateField.isDefined && datePattern.isDefined) {
					try DateTimeFormat.forPattern(datePattern.get).isParser
					catch {
						case _: IllegalArgumentException =>
							log.error(s"DS-JEDAI: Pattern \'${datePattern.get}\' is not valid")
							false
					}
				}
				else dateField.isEmpty
			if (!dateCheck) log.error("DS-JEDAI: Date pattern is not specified")
			val extension = getExtension
			// in case of CSV, TSV and SHP users must specify id
			val idCheck = extension match {
				case FileTypes.CSV | FileTypes.TSV | FileTypes.SHP | FileTypes.GEOJSON if realIdField.isEmpty =>
					log.error("DS-JEDAI: ID field is not specified")
					false
				case _ => true
			}

			// geometry field is necessary for all fileTypes except SHP
			val geometryCheck = extension match {
				case FileTypes.SHP | FileTypes.GEOJSON => true
				case _ => ! geometryField.isEmpty
			}
			if (!geometryCheck) log.error("DS-JEDAI: Geometry field is not specified")

			idCheck && geometryCheck && dateCheck
		}
	}
}

/**
 * main configuration class
 * @param source source dataset configurations
 * @param target target dataset configurations
 * @param relation examined relation
 * @param configurations execution configurations
 */
case class Configuration(source: DatasetConfigurations, target:DatasetConfigurations, relation: String, var configurations: Map[String, String]){

	def getSource: String = source.path

	def getTarget: String = target.path

	def getRelation: Relation= Relation.withName(relation)

	def getPartitions: Int = configurations.getOrElse(YamlConfiguration.CONF_PARTITIONS, "-1").toInt

	def getTheta: ThetaOption = ThetaOption.withName(configurations.getOrElse(YamlConfiguration.CONF_THETA_GRANULARITY, "avg"))

	def getMainWS: WeightingScheme = WeightingScheme.withName(configurations.getOrElse(YamlConfiguration.CONF_MAIN_WS, "JS"))

	def getSecondaryWS: Option[WeightingScheme] = configurations.get(YamlConfiguration.CONF_SECONDARY_WS) match {
		case Some(ws) => Option(WeightingScheme.withName(ws))
		case None => None
	}

	def getGridType: GridType = GridType.withName(configurations.getOrElse(YamlConfiguration.CONF_GRIDTYPE, "QUADTREE"))

	def getBudget: Int = configurations.getOrElse(YamlConfiguration.CONF_BUDGET, "0").toInt

	def getProgressiveAlgorithm: ProgressiveAlgorithm = ProgressiveAlgorithm.withName(configurations.getOrElse(YamlConfiguration.CONF_PROGRESSIVE_ALG, "PROGRESSIVE_GIANT"))

}

/**
 * basic Yaml parsers
 */
object ConfigurationYAML extends DefaultYamlProtocol {
	implicit val DatasetFormat = yamlFormat5(DatasetConfigurations)
	implicit val ConfigurationFormat = yamlFormat4(Configuration)
}

/**
 * Yaml Configuration Parser
 */
object ConfigurationParser {

	import ConfigurationYAML._
	val log: Logger = LogManager.getRootLogger

	/**
	 * check if the input relation is valid
	 * @param relation input relation
	 * @return true if relation is valid
	 */
	def checkRelation(relation: String): Boolean ={
		val valid = Relation.exists(relation)
		if (! valid) log.error("DS-JEDAI: Not Supported Relation")
		valid
	}


	/**
	 * check if the configurations are valid
	 * @param configurations input configuration
	 * @return true if configurations are valid
	 */
	def checkConfigurationMap(configurations: Map[String, String]): Boolean = {
		configurations.keys.foreach {
			key =>
				val value = configurations(key)
				key match {
					case YamlConfiguration.CONF_PARTITIONS =>
						if (! (value forall Character.isDigit)) {
							log.error("DS-JEDAI: Partitions must be an Integer")
							false
						}
					case YamlConfiguration.CONF_THETA_GRANULARITY =>
						if (!ThetaOption.exists(value)) {
							log.error("DS-JEDAI: Not valid measure for theta")
							false
						}
					case YamlConfiguration.CONF_BUDGET =>
						val allDigits = value forall Character.isDigit
						if (!allDigits) {
							log.error("DS-JEDAI: Not valid value for budget")
							false
						}
					case YamlConfiguration.CONF_PROGRESSIVE_ALG =>
						if (!ProgressiveAlgorithm.exists(value)) {
							log.error(s"DS-JEDAI: Prioritization Algorithm \'$value\' is not supported")
							false
						}
					case YamlConfiguration.CONF_MAIN_WS | YamlConfiguration.CONF_SECONDARY_WS=>
						if (! WeightingScheme.exists(value)) {
							log.error(s"DS-JEDAI: Weighting Scheme \'$value\' is not supported")
							false
						}
					case YamlConfiguration.CONF_GRIDTYPE=>
						if (! GridType.exists(value)){
							log.error(s"DS-JEDAI: Grid Type \'$value\' is not supported")
							false
						}
				}
		}
		true
	}


	/**
	 * terminates the process if finds Errors in configuration
	 * @param conf parsed yaml configuration
	 */
	def checkConfigurations(conf: Configuration): Unit ={
		val sourceCheck = conf.source.check()
		val targetCheck = conf.target.check()
		val relationCheck =  checkRelation(conf.relation)
		val confCheck = checkConfigurationMap(conf.configurations)

		if (! (sourceCheck && targetCheck && relationCheck && confCheck))
			System.exit(1)
	}

	/**
	 * parses Yaml file
	 * @param confPath path to yaml configuration file
	 * @return parsed Configuration
	 */
	def parse(confPath:String): Configuration ={
		val yamlStr = SparkContext.getOrCreate().textFile(confPath).collect().mkString("\n")
		val conf = yamlStr.parseYaml.convertTo[Configuration]
		checkConfigurations(conf)

		conf
	}

}