package experiments

import java.util.Calendar

import EntityMatching.PartitionMatching.{PartitionMatching, PartitionMatchingFactory}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Constants.MatchingAlgorithm
import utils.{ConfigurationParser, Utils}
import utils.Readers.SpatialReader

object PartitionExp {

    def main(args: Array[String]): Unit = {
        val startTime = Calendar.getInstance().getTimeInMillis
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
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
        val partitions: Int = conf.getPartitions

        // Loading Source
        SpatialReader.setPartitions(partitions)
        SpatialReader.noConsecutiveID()
        SpatialReader.setGridType(conf.getGridType)
        val sourceRDD = SpatialReader.load(conf.source.path, conf.source.realIdField, conf.source.geometryField)
            .setName("SourceRDD").persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = sourceRDD.count().toInt
        log.info("DS-JEDAI: Number of profiles of Source: " + sourceCount + " in " + sourceRDD.getNumPartitions +" partitions")

        // Loading Target
        val targetRDD = SpatialReader.load(conf.target.path, conf.source.realIdField, conf.source.geometryField)
            .setName("TargetRDD").persist(StorageLevel.MEMORY_AND_DISK)
        val targetCount = targetRDD.count().toInt
        log.info("DS-JEDAI: Number of profiles of Target: " + targetCount + " in " + targetRDD.getNumPartitions +" partitions")

        val (source, target, relation) = Utils.swappingStrategy(sourceRDD, targetRDD, conf.getRelation, sourceCount, targetCount)

        val budget = conf.getBudget
        val matching_startTime = Calendar.getInstance().getTimeInMillis
        val matcher = PartitionMatchingFactory.getMatchingAlgorithm(conf, source, target)
        if (conf.getMatchingAlgorithm == MatchingAlgorithm.SPATIAL) {
            val matches = matcher.apply(relation)
            log.info("DS-JEDAI: Matches: " + matches.count)
        }
        else{
            log.info("DS-JEDAI: With budget " + budget + " found: " +  matcher.applyWithBudget(relation, budget) )
        }


        val matching_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }

}
