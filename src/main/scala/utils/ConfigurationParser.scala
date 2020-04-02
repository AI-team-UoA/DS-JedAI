package utils

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._
import org.apache.log4j.{Level, LogManager, Logger}
import scala.io.Source

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

case class Dataset(path: String, realIdField: String, geometryField: String)

case class Configuration(source: Dataset, target:Dataset, relation: String, var configurations: Map[String, String])

object ConfigurationYAML extends DefaultYamlProtocol {
	implicit val DatasetFormat = yamlFormat3(Dataset)
	implicit val ConfigurationFormat = yamlFormat4(Configuration)
}

object ConfigurationParser {

	import ConfigurationYAML._
	Logger.getLogger("org").setLevel(Level.ERROR)
	Logger.getLogger("akka").setLevel(Level.ERROR)
	val log: Logger = LogManager.getRootLogger

	def checkRelation(relation: String): Unit ={
			val valid =
				relation == Constants.CONTAINS || relation == Constants.CROSSES || relation == Constants.DISJOINT ||
				relation == Constants.EQUALS || relation == Constants.INTERSECTS || relation == Constants.OVERLAPS ||
				relation == Constants.TOUCHES  || relation == Constants.WITHIN || relation == Constants.COVEREDBY ||
				relation == Constants.COVERS
		if (! valid){
			log.error("DS-JEDAI: Not Supported Relation")
			System.exit(1)
		}
	}

	def checkThetaMeasure(theta_measure: String): Boolean ={
		theta_measure == Constants.AVG || theta_measure == Constants.MAX ||
		theta_measure == Constants.MIN || theta_measure == Constants.NO_USE
	}

	def checkConfigurationMap(configurations: Map[String, String]): Unit = {
		configurations.keys.foreach {
			key =>
				val value = configurations(key)
				key match {
					case Constants.CONF_PARTITIONS =>
						if (! (value forall Character.isDigit)) {
							log.error("DS-JEDAI: Partitions must be an Integer")
							System.exit(1)
						}
					case Constants.CONF_BLOCK_ALG =>
						if (! (value == Constants.RADON || value == Constants.STATIC_BLOCKING || value == Constants.LIGHT_RADON)) {
							log.error("DS-JEDAI: Blocking Algorithm '" + value + "' not supported")
							System.exit(1)
						}
					case Constants.CONF_SPATIAL_BLOCKING_FACTOR =>
						if (! (value forall Character.isDigit)) {
							log.error("DS-JEDAI: Spatial Blocking Factor must be an Integer")
							System.exit(1)
						}
					case Constants.CONF_STATIC_BLOCKING_DISTANCE =>
						if (! value.matches("[+-]?\\d+.?\\d+")){
							log.error("DS-JEDAI: Static Blocking's distance must be a Number")
							System.exit(1)
						}
					case Constants.CONF_SPATIAL_PARTITION =>
						if (!(value == "false" || value == "true")) {
							log.error("DS-JEDAI: 'spatialPartition' must be Boolean")
							System.exit(1)
						}
					case Constants.CONF_THETA_MEASURE =>
						if (!checkThetaMeasure(value)) {
							log.error("DS-JEDAI: Not valid measure for theta")
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
		val bufferedConf = Source.fromFile(conf_path)
		val yaml_str = bufferedConf.getLines().mkString("\n")
		val conf = yaml_str.parseYaml.convertTo[Configuration]
		checkConfigurations(conf)

		conf
	}
}