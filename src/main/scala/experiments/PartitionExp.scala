package experiments

import java.util.Calendar

import EntityMatching.PartitionMatching.PartitionMatchingFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.{ConfigurationParser, Utils}
import utils.Readers.SpatialReader

object PartitionExp {
    def main(args: Array[String]): Unit = {
        val startTime = Calendar.getInstance().getTimeInMillis
        Logger.getLogger("org").setLevel(Level.INFO)
        Logger.getLogger("akka").setLevel(Level.INFO)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val sparkConf = new SparkConf()
            .setAppName("DS-JedAI")
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

        val conf_path = options("conf")
        val conf = ConfigurationParser.parse(conf_path)

        // Loading Source
        val sourceRDD = SpatialReader.load(conf.source.path, conf.source.realIdField, conf.source.geometryField)
            .setName("SourceRDD")persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = sourceRDD.count().toInt
        log.info("DS-JEDAI: Number of profiles of Source: " + sourceCount)

        // Loading Target
        val targetRDD = SpatialReader.load(conf.target.path, conf.source.realIdField, conf.source.geometryField)
            .setName("TargetRDD")persist(StorageLevel.MEMORY_AND_DISK)
        val targetCount = targetRDD.count().toInt
        log.info("DS-JEDAI: Number of profiles of Target: " + targetCount)

        val (source, target, relation) = Utils.swappingStrategy(sourceRDD, targetRDD, conf.relation, sourceCount, targetCount)

        val budget = 30000
        val matching_startTime = Calendar.getInstance().getTimeInMillis
        val matches = PartitionMatchingFactory.getMatchingAlgorithm(conf, source, target, budget, targetCount).apply(relation)

        log.info("DS-JEDAI: Matches: " + matches.count)
        val matching_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)

    }

}
