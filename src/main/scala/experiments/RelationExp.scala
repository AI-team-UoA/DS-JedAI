package experiments

import java.util.Calendar

import EntityMatching.DistributedMatching.DMFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Readers.SpatialReader
import utils.{ConfigurationParser, Utils}


object RelationExp {

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

        val reader = SpatialReader(conf.source, partitions)
        val sourceRDD = reader.load2PartitionedRDD()
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        Utils(sourceRDD.map(_._2.mbb), conf.getTheta, reader.partitionsZones)

        val targetRDD = reader.load2PartitionedRDD(conf.target)
        val partitioner = reader.partitioner

        val matching_startTime = Calendar.getInstance().getTimeInMillis
        val matches = DMFactory.getMatchingAlgorithm(conf, sourceRDD, targetRDD, partitioner).apply(conf.getRelation)

        log.info("DS-JEDAI: Matches: " + matches.count())
        val matching_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }

}