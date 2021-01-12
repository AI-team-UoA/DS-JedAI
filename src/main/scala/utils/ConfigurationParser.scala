package utils

import java.nio.file.{Files, Paths}

import net.jcazevedo.moultingyaml.{DefaultYamlProtocol, _}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.joda.time.format.DateTimeFormat
import utils.Constants.BlockingAlgorithm.BlockingAlgorithm
import utils.Constants.GridType.GridType
import utils.Constants.MatchingAlgorithm.MatchingAlgorithm
import utils.Constants.Relation.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
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
		if (! Files.exists(Paths.get(path))){
			log.error(s"DS-JEDAI: Path: \'$path\' does not exist")
			false
		}
		else if (geometryField == ""){
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
				case FileTypes.CSV | FileTypes.TSV | FileTypes.SHP if realIdField.isEmpty =>
					log.error("DS-JEDAI: ID field is not specified")
					false
				case _ => true
			}

			// geometry field is necessary for all fileTypes except SHP
			val geometryCheck = extension match {
				case FileTypes.SHP => true
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

	def getTheta: ThetaOption = ThetaOption.withName(configurations.getOrElse(YamlConfiguration.CONF_THETA_MEASURE, "NO_USE"))

	def getWeightingScheme: WeightStrategy = WeightStrategy.withName(configurations.getOrElse(YamlConfiguration.CONF_WEIGHTING_STRG, "CBS"))

	def getGridType: GridType = GridType.withName(configurations.getOrElse(YamlConfiguration.CONF_GRIDTYPE, "QUADTREE"))

	def getBudget: Int = configurations.getOrElse(YamlConfiguration.CONF_BUDGET, "10000").toInt

	def getSpatialPartitioning: Boolean = configurations.getOrElse(YamlConfiguration.CONF_SPATIAL_PARTITION, "false").toBoolean

	def getMatchingAlgorithm: MatchingAlgorithm = MatchingAlgorithm.withName(configurations.getOrElse(YamlConfiguration.CONF_MATCHING_ALG, "GIANT"))

	def getBlockingAlgorithm: BlockingAlgorithm = BlockingAlgorithm.withName(configurations.getOrElse(YamlConfiguration.CONF_BLOCK_ALG, "RADON"))

	def getBlockingFactor: Int = configurations.getOrElse(YamlConfiguration.CONF_SPATIAL_BLOCKING_FACTOR, "10").toInt

	def getBlockingDistance: Double = configurations.getOrElse(YamlConfiguration.CONF_STATIC_BLOCKING_DISTANCE, "0.0").toDouble

	def partitionBySource: Boolean =  configurations.getOrElse(YamlConfiguration.CONF_PARTITION_BY, Constants.DT_SOURCE)  == Constants.DT_SOURCE

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
					case YamlConfiguration.CONF_BLOCK_ALG =>
						if (! BlockingAlgorithm.exists(value)) {
							log.error(s"DS-JEDAI: Blocking Algorithm \'$value\' is not supported")
							false
						}
					case YamlConfiguration.CONF_SPATIAL_BLOCKING_FACTOR =>
						if (!(value forall Character.isDigit)) {
							log.error("DS-JEDAI: Spatial Blocking Factor must be an Integer")
							false
						}
					case YamlConfiguration.CONF_STATIC_BLOCKING_DISTANCE =>
						if (! value.matches("[+-]?\\d+.?\\d+")){
							log.error("DS-JEDAI: Static Blocking's distance must be a Number")
							false
						}
					case YamlConfiguration.CONF_THETA_MEASURE =>
						if (!ThetaOption.exists(value)) {
							log.error("DS-JEDAI: Not valid measure for theta")
							false
						}
					case YamlConfiguration.CONF_BUDGET =>
						val allDigits = value forall Character.isDigit
						if (!allDigits) {
							log.error("DS-JEDAI: Not valid measure for budget")
							false
						}
					case YamlConfiguration.CONF_MATCHING_ALG =>
						if (!MatchingAlgorithm.exists(value)) {
							log.error(s"DS-JEDAI: Prioritization Algorithm \'$value\' is not supported")
							false
						}
					case YamlConfiguration.CONF_WEIGHTING_STRG =>
						if (! WeightStrategy.exists(value)) {
							log.error(s"DS-JEDAI: Weighting algorithm \'$value\' is not supported")
							false
						}
					case YamlConfiguration.CONF_GRIDTYPE=>
						if (! GridType.exists(value)){
							log.error(s"DS-JEDAI: Grid Type \'$value\' is not supported")
							false
						}

					case YamlConfiguration.CONF_PARTITION_BY=>
						if (!(value == Constants.DT_SOURCE || value == Constants.DT_TARGET)) {
							log.error("DS-JEDAI: Error in partitionBy conf, accepted values are \"source\" or \"target\"")
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