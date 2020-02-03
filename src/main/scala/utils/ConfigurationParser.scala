package utils

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import net.jcazevedo.moultingyaml._
import org.apache.log4j.{Level, LogManager, Logger}

import scala.io.Source


case class Dataset(path: String, realIdField: String, geometryField: String)

case class Configuration(source: Dataset, target:Dataset, relation: String)

object ConfigurationYAML extends DefaultYamlProtocol {
	implicit val DatasetFormat = yamlFormat3(Dataset)
	implicit val ConfigurationFormat = yamlFormat3(Configuration)
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

	def parse(conf_path:String): Configuration ={
		val bufferedConf = Source.fromFile(conf_path)
		val yaml_str = bufferedConf.getLines().mkString("\n")
		val conf = yaml_str.parseYaml.convertTo[Configuration]

		if (!checkRelation(conf.relation)) {
			log.error("DS-JEDAI: Not Supported Relation")
			System.exit(1)
		}
		conf
	}
}