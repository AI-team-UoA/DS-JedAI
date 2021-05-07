package utils.configurationParser

import org.apache.log4j.{LogManager, Logger}
import org.joda.time.format.DateTimeFormat
import utils.Constants
import utils.Constants.FileTypes.FileTypes
import utils.Constants.GridType.GridType
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.Relation.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightingFunction.WeightingFunction
import utils.Constants._

/**
 * Configuration Interface
 */
sealed trait ConfigurationT {

    val relation: String
    val configurations: Map[String, String]

    def getRelation: Relation= Relation.withName(relation)

    def getPartitions: Int = configurations.getOrElse(YamlConfiguration.CONF_PARTITIONS, "-1").toInt

    def getTheta: ThetaOption = ThetaOption.withName(configurations.getOrElse(YamlConfiguration.CONF_THETA_GRANULARITY, "avg"))

    def getMainWF: WeightingFunction = WeightingFunction.withName(configurations.getOrElse(YamlConfiguration.CONF_MAIN_WF, "JS"))

    def getSecondaryWF: Option[WeightingFunction] = configurations.get(YamlConfiguration.CONF_SECONDARY_WF) match {
        case Some(wf) => Option(WeightingFunction.withName(wf))
        case None => None
    }

    def getWS: WeightingScheme = {
        val ws = configurations.getOrElse(YamlConfiguration.CONF_WS, "SINGLE")
        Constants.WeightingSchemeFactory(ws)
    }

    def getGridType: GridType = GridType.withName(configurations.getOrElse(YamlConfiguration.CONF_GRIDTYPE, "QUADTREE"))

    def getBudget: Int = configurations.getOrElse(YamlConfiguration.CONF_BUDGET, "0").toInt

    def getProgressiveAlgorithm: ProgressiveAlgorithm = ProgressiveAlgorithm.withName(configurations.getOrElse(YamlConfiguration.CONF_PROGRESSIVE_ALG, "PROGRESSIVE_GIANT"))

    def getOutputPath: String = configurations.getOrElse(YamlConfiguration.OUTPUT, "")
}


/**
 * main configuration class
 * @param source source dataset configurations
 * @param target target dataset configurations
 * @param relation examined relation
 * @param configurations execution configurations
 */
case class Configuration(source: DatasetConfigurations, target:DatasetConfigurations, relation: String, configurations: Map[String, String]) extends ConfigurationT {

    def getSource: String = source.path
    def getTarget: String = target.path
}


/**
 * Dirty configuration class - only one dataset
 * @param source source dataset configurations
 * @param relation examined relation
 * @param configurations execution configurations
 */
case class DirtyConfiguration(source: DatasetConfigurations, relation: String, configurations: Map[String, String]) extends  ConfigurationT {

    def getSource: String = source.path
}


/**
 * Input Dataset Configuration
 *
 * @param path input path
 * @param geometryField field of geometry
 * @param realIdField field of id (if it's not RDF) (optional)
 * @param dateField field of date (optional)
 * @param datePattern date pattern (optional, requisite if date field is given)
 */
case class DatasetConfigurations(path: String, geometryField: String, realIdField: Option[String] = None, dateField: Option[String] = None, datePattern: Option[String] = None){

    def getExtension: FileTypes = FileTypes.withName(path.toString.split("\\.").last)

    /**
     * check if the date field and pattern are specified, and if the pattern is valid
     * @return true if date fields are set correctly
     */
    def checkDateField: Boolean = {
        if (dateField.isDefined) {
            val correctFields = dateField.nonEmpty && datePattern.isDefined && datePattern.nonEmpty
            if (correctFields)
                try DateTimeFormat.forPattern(datePattern.get).isParser
                catch {
                    case _: IllegalArgumentException => false
                }
            else false
        }
        else true
    }

    /**
     * check id field
     * @return true if id is set correctly
     */
    def checkIdField: Boolean = getExtension match {
        case FileTypes.CSV | FileTypes.TSV | FileTypes.SHP | FileTypes.GEOJSON if realIdField.isEmpty => false
        case _ => true
    }

    /**
     * check geometry field
     * @return true if geometry field is set correctly
     */
    def checkGeometryField: Boolean = getExtension match {
        case FileTypes.SHP | FileTypes.GEOJSON => true
        case _ => geometryField.nonEmpty
    }

    /**
     * check if dataset configuration is set correctly
     * @return true f dataset configuration is set correctly
     */
    def check:Boolean ={
        val pathCheck = path.nonEmpty
        val dateCheck = checkDateField
        val idCheck = checkIdField
        val geometryCheck = checkGeometryField

        val log: Logger = LogManager.getRootLogger
        if (!pathCheck) log.error(s"DS-JEDAI: Input path is not defined")
        if (!dateCheck) log.error(s"DS-JEDAI: Date field is not set correctly")
        if (!idCheck) log.error(s"DS-JEDAI: ID field is not set correctly")
        if (!geometryCheck) log.error(s"DS-JEDAI: Geometry field is not set correctly")

        pathCheck && dateCheck && idCheck && geometryCheck
    }
}