package utils.configuration

import net.jcazevedo.moultingyaml.{DefaultYamlProtocol, _}
import org.apache.spark.SparkContext
import utils.configuration.Constants.{InputConfigurations, ProgressiveAlgorithm, ThetaOption, WeightingFunction, _}

/**
 * Yaml parsers
 */


/**
 * Yaml Configuration Parser
 */
class ConfigurationParser {

	object ConfigurationYAML extends DefaultYamlProtocol {
		implicit val DatasetFormat = yamlFormat5(DatasetConfigurations)
		implicit val ConfigurationFormat = yamlFormat4(Configuration)
		implicit val DirtyConfigurationFormat = yamlFormat3(DirtyConfiguration)
	}

	import ConfigurationYAML._

	/**
	 * Parsing input arguments (command line + yaml) into Configuration
	 *
	 * @param args command line arguments
	 * @return either a list of Errors or a Configuration
	 */
	def parse(args: Seq[String]): Either[List[ConfigurationErrorMessage], Configuration] =
		parseCommandLineArguments(args) match {
			case Left(errors) 		=> Left(errors)
			case Right(options) 	=>

				val confPathOpt = options.get(InputConfigurations.CONF_CONFIGURATIONS)
				confPathOpt match {
					// there is configuration file
					case Some(confPath) =>
						parseConfigurationFile(confPath) match {
							case Left(errors) 		  => Left(errors)
							case Right(configuration) =>
								configuration.combine(options)
								Right(configuration)
						}
					// there is no conf file so we check the CLI arguments
					case None =>
						val sourcePathOpt = options.get(InputConfigurations.CONF_SOURCE)
						val targetPathOpt = options.get(InputConfigurations.CONF_TARGET)
						val relationOpt = options.get(InputConfigurations.CONF_RELATION)

						(sourcePathOpt, targetPathOpt, relationOpt) match {
							// necessary CLI arguments were provided
							case (Some(sourcePath), Some(targetPath), Some(relation)) =>
								val geometryPredicate = options.getOrElse(InputConfigurations.CONF_GEOMETRY_PREDICATE, Constants.geometryPredicate)
								val source = DatasetConfigurations(sourcePath, geometryPredicate)
								val target = DatasetConfigurations(targetPath, geometryPredicate)
								val configuration = Configuration(source, target, relation, options)
								Right(configuration)

							// No CLI arguments and no configuration file were provided
							case _ =>
								val error = ConfigurationErrorMessage("No Source or Target file was specified")
								Left(error :: Nil)
						}
				}
		}


	/**
	 * Parse Yaml configuration file
	 * @param confPath path to yaml configuration file
	 * @return either a list of errors or parsed Configuration
	 */
	def parseConfigurationFile(confPath:String): Either[List[ConfigurationErrorMessage], Configuration] ={
		val yamlStr = SparkContext.getOrCreate().textFile(confPath).collect().mkString("\n")
		val conf = yamlStr.parseYaml.convertTo[Configuration]
		checkConfiguration(conf) match {
			case None 			=> Right(conf)
			case Some(errors) 	=> Left(errors)
		}
	}


	/**
	 * Parse Yaml configuration file fitting for dirty execution
	 * @param confPath path to yaml configuration file
	 * @return either a list of errors or parsed Configuration
	 */
	def parseDirty(confPath: String): Either[List[ConfigurationErrorMessage], DirtyConfiguration] ={
		val yamlStr = SparkContext.getOrCreate().textFile(confPath).collect().mkString("\n")
		val conf = yamlStr.parseYaml.convertTo[DirtyConfiguration]
		checkConfiguration(conf) match {
			case None 			=> Right(conf)
			case Some(errors) 	=> Left(errors)
		}
	}


	/**
	 * Parse command line arguments
	 * @param args command line arguments
	 * @return either a list of errors or parsed Configuration
	 */
	def parseCommandLineArguments(args: Seq[String]): Either[List[ConfigurationErrorMessage], Map[String, String]] ={
		// Parsing input arguments
		@scala.annotation.tailrec
		def nextOption(map: Map[String, String], list: List[String]): Map[String, String] = {
			list match {
				case Nil => map
				case ("-c" | "-conf") :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_CONFIGURATIONS -> value), tail)
				case ("-p" | "-partitions") :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_PARTITIONS -> value), tail)
				case "-source" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_SOURCE -> value), tail)
				case "-target" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_TARGET -> value), tail)
				case "-relation" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_RELATION -> value), tail)
				case "-geometryPredicate" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_GEOMETRY_PREDICATE -> value), tail)
				case "-gt" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_GRID_TYPE -> value), tail)
				case "-stats" :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_STATISTICS -> "true"), tail)
				case "-o" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_OUTPUT -> value), tail)
				case "-et" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_ENTITY_TYPE -> value), tail)
				case "-gat" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_GEOMETRY_APPROXIMATION_TYPE -> value), tail)
				case ("-b" | "-budget") :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_BUDGET -> value), tail)
				case "-pa" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_PROGRESSIVE_ALG -> value), tail)
				case "-mwf" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_MAIN_WF -> value), tail)
				case "-swf" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_SECONDARY_WF -> value), tail)
				case "-ws" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_WS -> value), tail)
				case "-tv" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_TOTAL_VERIFICATIONS -> value), tail)
				case "-qp" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_QUALIFYING_PAIRS -> value), tail)
				case "-dcmpT" :: value :: tail =>
					nextOption(map ++ Map(InputConfigurations.CONF_DECOMPOSITION_THRESHOLD -> value), tail)
				case key :: tail =>
					nextOption(Map(InputConfigurations.CONF_UNRECOGNIZED -> key), tail)
			}
		}

		val argList = args.toList
		val conf = nextOption(Map(), argList)

		val errorsOpt: List[Option[ConfigurationErrorMessage]] =  checkConfigurationPath(conf) :: checkConfigurationMap(conf)
		// ETW: extract errors, transform/clean, wrap them as option
		val errors: Option[List[ConfigurationErrorMessage]] = Option(errorsOpt.flatten).filter(_.nonEmpty)
		errors match {
			case None 			=> Right(conf)
			case Some(errors) 	=> Left(errors)
		}
	}

	/**
	 * Check the input configurations
	 * @param conf configurations
	 * @return an Option list of ErrorMessages
	 */
	def checkConfiguration(conf: ConfigurationT): Option[List[ConfigurationErrorMessage]] ={
		val errors: List[Option[ConfigurationErrorMessage]] = conf match {
			case DirtyConfiguration(source, relation, configurations) =>
				checkRelation(relation) :: (source.check ::: checkConfigurationMap(configurations))
			case Configuration(source, target, relation, configurations) =>
				checkRelation(relation) :: (source.check ::: target.check ::: checkConfigurationMap(configurations))
		}
		// ETW: extract errors, transform/clean, wrap them as option
		Option(errors.flatten).filter(_.nonEmpty)
	}

	/**
	 * Check if conf contains the path to the Yaml Configuration file
	 * @param conf a Map of arguments
	 * @return an Option ErrorMessage if yaml path is not in the Map of arguments
	 */
	def checkConfigurationPath(conf: Map[String, String]): Option[ConfigurationErrorMessage] =
		conf.get(InputConfigurations.CONF_CONFIGURATIONS) match {
			case Some(_) => None
			case None if conf.contains(InputConfigurations.CONF_SOURCE) && conf.contains(InputConfigurations.CONF_TARGET) => None
			case None => Some(ConfigurationErrorMessage(s"Path to configuration file is not provided"))
		}


	/**
	 * Check if the input relation is valid
	 * @param relation input relation
	 * @return an Option ErrorMessage if the relation is not valid
	 */
	def checkRelation(relation: String): Option[ConfigurationErrorMessage] =
		if (Relation.exists(relation)) None
		else Some(ConfigurationErrorMessage(s"Relation '$relation' is not supported'"))


	/**
	 * Check if the loaded configurations are valid
	 * @param configurations input configuration
	 * @return a list of optional errors
	 */
	def checkConfigurationMap(configurations: Map[String, String]): List[Option[ConfigurationErrorMessage]] = {
		configurations.keys.map { key =>
			val value = configurations(key)
			key match {
				case InputConfigurations.CONF_PARTITIONS if !(value forall Character.isDigit) =>
					Some(ConfigurationErrorMessage("Partitions must be an Integer"))
				case InputConfigurations.CONF_THETA_GRANULARITY if !ThetaOption.exists(value) =>
					Some(ConfigurationErrorMessage("Not valid measure for theta"))
				case InputConfigurations.CONF_BUDGET if !(value forall Character.isDigit) =>
					Some(ConfigurationErrorMessage("Not valid value for budget"))
				case InputConfigurations.CONF_PROGRESSIVE_ALG if !ProgressiveAlgorithm.exists(value) =>
					Some(ConfigurationErrorMessage(s"Prioritization Algorithm '$value' is not supported"))
				case InputConfigurations.CONF_MAIN_WF | InputConfigurations.CONF_SECONDARY_WF if !WeightingFunction.exists(value) =>
					Some(ConfigurationErrorMessage(s"Weighting Function '$value' is not supported"))
				case InputConfigurations.CONF_GRID_TYPE if !GridType.exists(value) =>
					Some(ConfigurationErrorMessage(s"Grid Type '$value' is not supported"))
				case InputConfigurations.CONF_WS if !Constants.checkWS(value) =>
					Some(ConfigurationErrorMessage(s"Weighting Scheme '$value' is not supported"))
				case InputConfigurations.CONF_ENTITY_TYPE if !EntityTypeENUM.exists(value) =>
					Some(ConfigurationErrorMessage(s"Entity Type '$value' is not supported"))
				case InputConfigurations.CONF_GEOMETRY_APPROXIMATION_TYPE if !GeometryApproximationENUM.exists(value) =>
					Some(ConfigurationErrorMessage(s"Geometry Approximation Type '$value' is not supported"))
				case InputConfigurations.CONF_DECOMPOSITION_THRESHOLD if !(value forall Character.isDigit) =>
					Some(ConfigurationErrorMessage("Not valid value for threshold"))
				case InputConfigurations.CONF_UNRECOGNIZED =>
					Some(ConfigurationErrorMessage(s"Unrecognized argument '$value'"))
				case _ => None
			}
		}.toList
	}
}