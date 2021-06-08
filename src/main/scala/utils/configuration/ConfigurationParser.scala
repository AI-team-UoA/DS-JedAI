package utils.configuration

import net.jcazevedo.moultingyaml.{DefaultYamlProtocol, _}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import utils.configuration.Constants._
import utils.configuration.Constants.{ProgressiveAlgorithm, ThetaOption, WeightingFunction, YamlConfiguration}

/**
 * Yaml parsers
 */
object ConfigurationYAML extends DefaultYamlProtocol {
	implicit val DatasetFormat = yamlFormat5(DatasetConfigurations)
	implicit val ConfigurationFormat = yamlFormat4(Configuration)
	implicit val DirtyConfigurationFormat = yamlFormat3(DirtyConfiguration)
}

/**
 * TODO add command line argument parsing here
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
		configurations.keys.foreach { key =>
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
				case YamlConfiguration.CONF_MAIN_WF | YamlConfiguration.CONF_SECONDARY_WF=>
					if (! WeightingFunction.exists(value)) {
						log.error(s"DS-JEDAI: Weighting Function \'$value\' is not supported")
						false
					}
				case YamlConfiguration.CONF_GRID_TYPE=>
					if (! GridType.exists(value)){
						log.error(s"DS-JEDAI: Grid Type \'$value\' is not supported")
						false
					}

				case YamlConfiguration.CONF_WS=>
					if (! Constants.checkWS(value)){
						log.error(s"DS-JEDAI: Weighting Scheme \'$value\' is not supported")
						false
					}

				case YamlConfiguration.CONF_ENTITY_TYPE=>
					if (! EntityTypeENUM.exists(value)){
						log.error(s"DS-JEDAI: Entity Type \'$value\' is not supported")
						false
					}
				case _ =>
			}
		}
		true
	}


	/**
	 * terminates the process if finds Errors in configuration
	 * @param conf parsed yaml configuration
	 */
	def checkConfigurationsOrTerminate(conf: ConfigurationT): Unit ={
		val isValid = conf match {
			case DirtyConfiguration(source, relation, configurations) =>
				source.check && checkRelation(relation) && checkConfigurationMap(configurations)
			case Configuration(source, target, relation, configurations) =>
				source.check && target.check && checkRelation(relation) && checkConfigurationMap(configurations)
		}
		if (!isValid) System.exit(1)
	}

	/**
	 * parses Yaml file
	 * @param confPath path to yaml configuration file
	 * @return parsed Configuration
	 */
	def parse(confPath:String): Configuration ={
		val yamlStr = SparkContext.getOrCreate().textFile(confPath).collect().mkString("\n")
		val conf = yamlStr.parseYaml.convertTo[Configuration]
		checkConfigurationsOrTerminate(conf)
		conf
	}


	/**
	 * parses Yaml file
	 * @param confPath path to yaml configuration file
	 * @return parsed Configuration
	 */
	def parseDirty(confPath:String): DirtyConfiguration ={
		val yamlStr = SparkContext.getOrCreate().textFile(confPath).collect().mkString("\n")
		val conf = yamlStr.parseYaml.convertTo[DirtyConfiguration]
		checkConfigurationsOrTerminate(conf)
		conf
	}

}