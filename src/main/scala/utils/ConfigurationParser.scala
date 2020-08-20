package utils

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import utils.Constants.MatchingAlgorithm.MatchingAlgorithm
import utils.Constants.BlockingAlgorithm.BlockingAlgorithm
import utils.Constants.GridType.GridType
import utils.Constants.Relation.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Constants.{BlockingAlgorithm, GridType, MatchingAlgorithm, Relation, ThetaOption, WeightStrategy, YamlConfiguration}

import scala.io.Source

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

case class Dataset(path: String, realIdField: String, geometryField: String)

case class Configuration(source: Dataset, target:Dataset, relation: String, var configurations: Map[String, String]){

	def getSource: String = source.path

	def getTarget: String = target.path

	def getRelation: Relation= Relation.withName(relation)

	def getPartitions: Int = configurations.getOrElse(YamlConfiguration.CONF_PARTITIONS, "-1").toInt

	def getTheta: ThetaOption = ThetaOption.withName(configurations.getOrElse(YamlConfiguration.CONF_THETA_MEASURE, "NO_USE"))

	def getWeightingScheme: WeightStrategy = WeightStrategy.withName(configurations.getOrElse(YamlConfiguration.CONF_WEIGHTING_STRG, "CBS"))

	def getGridType: GridType = GridType.withName(configurations.getOrElse(YamlConfiguration.CONF_GRIDTYPE, "QUADTREE"))

	def getBudget: Int = configurations.getOrElse(YamlConfiguration.CONF_BUDGET, "10000").toInt

	def getSpatialPartitioning: Boolean = configurations.getOrElse(YamlConfiguration.CONF_SPATIAL_PARTITION, "false").toBoolean

	def getMatchingAlgorithm: MatchingAlgorithm = MatchingAlgorithm.withName(configurations.getOrElse(YamlConfiguration.CONF_MATCHING_ALG, "SPATIAL"))

	def getBlockingAlgorithm: BlockingAlgorithm = BlockingAlgorithm.withName(configurations.getOrElse(YamlConfiguration.CONF_BLOCK_ALG, "RADON"))

	def getBlockingFactor: Int = configurations.getOrElse(YamlConfiguration.CONF_SPATIAL_BLOCKING_FACTOR, "10").toInt

	def getBlockingDistance: Double = configurations.getOrElse(YamlConfiguration.CONF_STATIC_BLOCKING_DISTANCE, "0.0").toDouble

	def partitionBySource: Boolean =  configurations.getOrElse(YamlConfiguration.CONF_PARTITION_BY, Constants.DT_SOURCE)  == Constants.DT_SOURCE


}

object ConfigurationYAML extends DefaultYamlProtocol {
	implicit val DatasetFormat = yamlFormat3(Dataset)
	implicit val ConfigurationFormat = yamlFormat4(Configuration)
}

object ConfigurationParser {

	import ConfigurationYAML._
	val log: Logger = LogManager.getRootLogger

	def checkRelation(relation: String): Unit ={
			val valid = Relation.exists(relation)
		if (! valid){
			log.error("DS-JEDAI: Not Supported Relation")
			System.exit(1)
		}
	}

	def checkThetaMeasure(theta_measure: String): Boolean ={
		ThetaOption.exists(theta_measure)
	}

	def checkConfigurationMap(configurations: Map[String, String]): Unit = {
		configurations.keys.foreach {
			key =>
				val value = configurations(key)
				key match {
					case YamlConfiguration.CONF_PARTITIONS =>
						if (! (value forall Character.isDigit)) {
							log.error("DS-JEDAI: Partitions must be an Integer")
							System.exit(1)
						}
					case YamlConfiguration.CONF_BLOCK_ALG =>
						if (! BlockingAlgorithm.exists(value)) {
							log.error("DS-JEDAI: Blocking Algorithm '" + value + "' is not supported")
							System.exit(1)
						}
					case YamlConfiguration.CONF_SPATIAL_BLOCKING_FACTOR =>
						if (!(value forall Character.isDigit)) {
							log.error("DS-JEDAI: Spatial Blocking Factor must be an Integer")
							System.exit(1)
						}
					case YamlConfiguration.CONF_STATIC_BLOCKING_DISTANCE =>
						if (! value.matches("[+-]?\\d+.?\\d+")){
							log.error("DS-JEDAI: Static Blocking's distance must be a Number")
							System.exit(1)
						}
					case YamlConfiguration.CONF_SPATIAL_PARTITION =>
						if (!(value == "false" || value == "true")) {
							log.error("DS-JEDAI: 'spatialPartition' must be Boolean")
							System.exit(1)
						}
					case YamlConfiguration.CONF_THETA_MEASURE =>
						if (!checkThetaMeasure(value)) {
							log.error("DS-JEDAI: Not valid measure for theta")
							System.exit(1)
						}
					case YamlConfiguration.CONF_BUDGET =>
						val allDigits = value forall Character.isDigit
						if (!allDigits) {
							log.error("DS-JEDAI: Not valid measure for budget")
							System.exit(1)
						}
					case YamlConfiguration.CONF_MATCHING_ALG =>
						if (!MatchingAlgorithm.exists(value)) {
							log.error("DS-JEDAI: Prioritization Algorithm '" + value + "' is not supported")
							System.exit(1)
						}
					case YamlConfiguration.CONF_WEIGHTING_STRG =>
						if (! WeightStrategy.exists(value)) {
							log.error("DS-JEDAI: Weighting algorithm '" + value + "' is not supported")
							System.exit(1)
						}
					case YamlConfiguration.CONF_GRIDTYPE=>
						if (! GridType.exists(value)){
							log.error("DS-JEDAI: Grid Type '" + value + "' is not supported")
							System.exit(1)
						}

					case YamlConfiguration.CONF_PARTITION_BY=>
						if (!(value == Constants.DT_SOURCE || value == Constants.DT_TARGET)) {
							log.error("DS-JEDAI: Error in partitionBy conf, accepted values are \"source\" or \"target\"")
							System.exit(1)
						}
				}
		}
	}

	def checkConfigurations(conf: Configuration): Unit ={
		checkRelation(conf.relation)
		checkConfigurationMap(conf.configurations)
	}

	def parse(conf_path:String): Configuration ={
		val yaml_str = SparkContext.getOrCreate().textFile(conf_path).collect().mkString("\n")
		val conf = yaml_str.parseYaml.convertTo[Configuration]
		checkConfigurations(conf)

		conf
	}

}