package experiments

import java.util.Calendar

import EntityMatching.PartitionMatching.{ComparisonCentricPrioritization, PartitionMatching}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.{ConfigurationParser, Constants, Utils}
import utils.Readers.{Reader, SpatialReader}

object SpatialExp {
    def main(args: Array[String]): Unit = {
        val startTime = Calendar.getInstance().getTimeInMillis
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val sparkConf = new SparkConf()
            .setAppName("SD-JedAI")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = new SparkContext(sparkConf)
        val spark: SparkSession = SparkSession.builder().getOrCreate()

        // Parsing the input arguments
        @scala.annotation.tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case ("-c" | "-conf") :: value :: tail =>
                    nextOption(map ++ Map("conf" -> value), tail)
                case _ :: tail =>
                    log.warn("DS-JEDAI: Unrecognized argument")
                    nextOption(map, tail)
            }
        }

        val arglist = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), arglist)

        if (!options.contains("conf")) {
            log.error("DS-JEDAI: No configuration file!")
            System.exit(1)
        }

        // TODO configuration arguments must return from configuration
        // TODO add a budget configuration
        val conf_path = options("conf")
        val conf = ConfigurationParser.parse(conf_path)
        val partitions: Int = conf.configurations.getOrElse(Constants.CONF_PARTITIONS, "-1").toInt

        // Loading Source
        val sourceRDD = SpatialReader.load(conf.source.path, conf.source.realIdField, conf.source.geometryField)
        val sourceCount = sourceRDD.setName("SourceRDD").cache().count().toInt
        log.info("DS-JEDAI: Number of profiles of Source: " + sourceCount)

        // Loading Target
        val targetRDD = SpatialReader.load(conf.target.path, conf.source.realIdField, conf.source.geometryField)
        val targetCount = targetRDD.setName("TargetRDD").cache().count().toInt
        log.info("DS-JEDAI: Number of profiles of Target: " + targetCount)

        val (source, target, relation) = Utils.swappingStrategy(sourceRDD, targetRDD, conf.relation)

        val theta_msr = conf.configurations.getOrElse(Constants.CONF_THETA_MEASURE, Constants.NO_USE)
        val weightingScheme = conf.configurations.getOrElse(Constants.CONF_WEIGHTING_STRG, Constants.CBS)

        val matching_startTime = Calendar.getInstance().getTimeInMillis
        val matches = PartitionMatching(source, target, weightingScheme, theta_msr).apply(relation)
        // val matches = ComparisonCentricPrioritization(source, target, weightingScheme, theta_msr).apply(relation)
        log.info("DS-JEDAI: Matches: " + matches.count)
        val matching_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)

    }

}
