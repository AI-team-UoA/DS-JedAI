package utils

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._
import org.apache.log4j.{Level, LogManager, Logger}

import scala.io.Source


case class Dataset(path: String, realIdField: String, geometryField: String)

case class Configuration(source: Dataset, target:Dataset, relation: String, theta_measure: String)

object ConfigurationYAML extends DefaultYamlProtocol {
	implicit val DatasetFormat = yamlFormat3(Dataset)
	implicit val ConfigurationFormat = yamlFormat4(Configuration)
}

object ConfigurationParser {

	import ConfigurationYAML._
	Logger.getLogger("org").setLevel(Level.ERROR)
	Logger.getLogger("akka").setLevel(Level.ERROR)
	val log: Logger = LogManager.getRootLogger

	def checkRelation(relation: String): Boolean ={
			relation == Constant.CONTAINS || relation == Constant.CROSSES || relation == Constant.DISJOINT ||
			relation == Constant.EQUALS || relation == Constant.INTERSECTS || relation == Constant.OVERLAPS ||
			relation == Constant.TOUCHES  || relation == Constant.WITHIN || relation == Constant.COVEREDBY ||
			relation == Constant.COVERS
	}

	def checkThetaMeasure(theta_measure: String): Boolean ={
		theta_measure == Constant.AVG || theta_measure == Constant.MAX ||
		theta_measure == Constant.MIN || theta_measure == Constant.NO_USE
	}

	def parse(conf_path:String): Configuration ={
		val bufferedConf = Source.fromFile(conf_path)
		val yaml_str = bufferedConf.getLines().mkString("\n")
		val conf = yaml_str.parseYaml.convertTo[Configuration]

		if (!checkRelation(conf.relation)) {
			log.error("DS-JEDAI: Not Supported Relation")
			System.exit(1)
		}

		if (!checkThetaMeasure(conf.theta_measure)) {
			log.error("DS-JEDAI: Not valid measure for theta")
			System.exit(1)
		}
		conf
	}
}