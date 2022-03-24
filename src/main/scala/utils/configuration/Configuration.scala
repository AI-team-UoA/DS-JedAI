package utils.configuration

import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat
import utils.configuration.Constants.EntityTypeENUM.{DECOMPOSED_ENTITY, EntityTypeENUM}
import utils.configuration.Constants.FileTypes.FileTypes
import utils.configuration.Constants.GeometryApproximationENUM.GeometryApproximationENUM
import utils.configuration.Constants.GridType.GridType
import utils.configuration.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.ThetaOption.ThetaOption
import utils.configuration.Constants.WeightingFunction.WeightingFunction
import utils.configuration.Constants._

import java.io.File

/**
 * Configuration Interface
 */

case class ConfigurationErrorMessage(message: String){
    def getMessage: String = s"DS-JEDAI: ERROR - $message"
}

sealed trait ConfigurationT {

    val relation: String
    var configurations: Map[String, String]

    def getRelation: Relation= Relation.withName(relation)

    def getPartitions: Int = configurations.getOrElse(InputConfigurations.CONF_PARTITIONS, "-1").toInt

    def getTheta: ThetaOption = ThetaOption.withName(configurations.getOrElse(InputConfigurations.CONF_THETA_GRANULARITY, "avg"))

    def getMainWF: WeightingFunction = WeightingFunction.withName(configurations.getOrElse(InputConfigurations.CONF_MAIN_WF, "JS"))

    def getSecondaryWF: Option[WeightingFunction] = configurations.get(InputConfigurations.CONF_SECONDARY_WF) match {
        case Some(wf) => Option(WeightingFunction.withName(wf))
        case None => None
    }

    def getWS: WeightingScheme = {
        val ws = configurations.getOrElse(InputConfigurations.CONF_WS, "SIMPLE")
        Constants.WeightingSchemeFactory(ws)
    }

    def getGridType: GridType = GridType.withName(configurations.getOrElse(InputConfigurations.CONF_GRID_TYPE, "QUADTREE"))

    def getBudget: Int = configurations.getOrElse(InputConfigurations.CONF_BUDGET, "0").toInt

    def getProgressiveAlgorithm: ProgressiveAlgorithm = ProgressiveAlgorithm.withName(configurations.getOrElse(InputConfigurations.CONF_PROGRESSIVE_ALG, "PROGRESSIVE_GIANT"))

    def getExportPath: Option[String] = configurations.get(InputConfigurations.CONF_EXPORT_PATH)

    def getEntityType: EntityTypeENUM = EntityTypeENUM.withName(configurations.getOrElse(InputConfigurations.CONF_ENTITY_TYPE, "SPATIAL_ENTITY"))

    def measureStatistic: Boolean = configurations.contains(InputConfigurations.CONF_STATISTICS)

    def getTotalVerifications: Option[Int] = configurations.get(InputConfigurations.CONF_TOTAL_VERIFICATIONS).map(_.toInt)

    def getTotalQualifyingPairs: Option[Int] = configurations.get(InputConfigurations.CONF_QUALIFYING_PAIRS).map(_.toInt)

    def getDecompositionThreshold: Option[Double] =
        if (configurations.contains(InputConfigurations.CONF_DECOMPOSITION_THRESHOLD))
            configurations.get(InputConfigurations.CONF_DECOMPOSITION_THRESHOLD).map(_.toDouble)
        else {
            getEntityType match {
                case DECOMPOSED_ENTITY =>
                    Some(configurations.getOrElse(InputConfigurations.CONF_DECOMPOSITION_THRESHOLD, "1").toDouble)
                case _ => None
            }
        }

    def getApproximationType: Option[GeometryApproximationENUM] =
        configurations.get(InputConfigurations.CONF_GEOMETRY_APPROXIMATION_TYPE).map(ga => GeometryApproximationENUM.withName(ga))

    def getBatchSize: Int =  configurations.getOrElse(InputConfigurations.CONF_BATCH_SIZE, "10000").toInt

    def getViolations: Int =  configurations.getOrElse(InputConfigurations.CONF_VIOLATIONS, "3").toInt

    def getPrecisionLimit: Float =  configurations.getOrElse(InputConfigurations.CONF_PRECISION_LIMIT, "0.1f").toFloat

    def print(log: Logger): Unit = {
        log.info(s"DS-JEDAI: GridType: $getGridType")
        log.info(s"DS-JEDAI: Relation: $relation")
        log.info(s"DS-JEDAI: Entity Type: $getEntityType")
        log.info(s"DS-JEDAI: Geometry Approximation: ${getApproximationType.getOrElse(GeometryApproximationENUM.MBR)}")
        getDecompositionThreshold.foreach(dcmpT => log.info(s"DS-JEDAI: Decomposition Threshold: $dcmpT"))
        getExportPath.foreach(path => log.info(s"DS-JEDAI: Export Path: $path"))
    }


    def printProgressive(log: Logger): Unit = {
        print(log)
        log.info(s"DS-JEDAI: Progressive Algorithm: $getProgressiveAlgorithm")
        log.info(s"DS-JEDAI: Weighting Scheme: $getWS")
        log.info(s"DS-JEDAI: Input Budget: $getBudget")
        if (getProgressiveAlgorithm == ProgressiveAlgorithm.EARLY_STOPPING) {
            log.info(s"DS-JEDAI: Main Weighting Function: JS")
            log.info(s"DS-JEDAI: Secondary Weighting Function: CF")
            log.info(s"DS-JEDAI: Last Weighting Function: MBRO")
            log.info(s"DS-JEDAI: BATCH SIZE: $getBatchSize")
            log.info(s"DS-JEDAI: PRECISION LIMIT: $getPrecisionLimit")
            log.info(s"DS-JEDAI: VIOLATIONS: $getViolations")
        }
        else {
            log.info(s"DS-JEDAI: Main Weighting Function: ${getMainWF}")
            getSecondaryWF.foreach(swf => log.info(s"DS-JEDAI: Secondary Weighting Function: $swf"))
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
case class Configuration(source: DatasetConfigurations, target:DatasetConfigurations, relation: String, var configurations: Map[String, String]) extends ConfigurationT {

    def getSource: String = source.path
    def getTarget: String = target.path

    def combine(conf: Map[String, String]): Unit =
        configurations = configurations ++ conf
}


/**
 * Dirty configuration class - only one dataset
 * @param source source dataset configurations
 * @param relation examined relation
 * @param configurations execution configurations
 */
case class DirtyConfiguration(source: DatasetConfigurations, relation: String, var configurations: Map[String, String]) extends  ConfigurationT {

    def getSource: String = source.path

    def combine(conf: Map[String, String]): Unit =
        configurations = configurations ++ conf
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

    def getExtension: Option[FileTypes] = {
        val extension = path.split("\\.").last
        if(FileTypes.exists(extension))
            Some(FileTypes.withName(extension))
        else
            None
    }

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
    def checkIdField: Boolean =
        getExtension match {
        case Some(FileTypes.CSV) | Some(FileTypes.TSV) | Some(FileTypes.SHP) | Some(FileTypes.GEOJSON) if realIdField.isEmpty => false
        case _ => true
    }

    /**
     * check geometry field
     * @return true if geometry field is set correctly
     */
    def checkGeometryField: Boolean = getExtension match {
        case Some(FileTypes.SHP) | Some(FileTypes.GEOJSON) => true
        case _ => geometryField.nonEmpty
    }

    /**
     * check if dataset configuration is set correctly
     * @return true f dataset configuration is set correctly
     */
    def check: List[Option[ConfigurationErrorMessage]] ={
        val pathCheck = if (path.nonEmpty) None else Some(ConfigurationErrorMessage(s"Input path  '$path' is not defined"))
        val dateCheck = if (checkDateField) None else Some(ConfigurationErrorMessage(s"Date field is not set correctly"))
        val idCheck = if (checkIdField) None else Some(ConfigurationErrorMessage(s"Id field is not set correctly"))
        val geometryCheck = if (checkGeometryField) None else Some(ConfigurationErrorMessage("Geometry field is not set correctly"))

        pathCheck :: dateCheck :: idCheck :: geometryCheck :: Nil
    }
}