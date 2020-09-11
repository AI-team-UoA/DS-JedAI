package experiments

import java.util.Calendar

import EntityMatching.PartitionMatching.PartitionMatchingFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Constants.Relation
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

        SpatialReader.setPartitions(partitions)
        SpatialReader.noConsecutiveID()
        SpatialReader.setGridType(conf.getGridType)

        // loading source and target, its very important the big dataset to be partitioned first as it will set the partitioner
        val (sourceRDD, targetRDD) = if (conf.partitionBySource){
            val sourceRDD = SpatialReader.load(conf.source.path, conf.source.realIdField, conf.source.geometryField)
                .setName("SourceRDD").persist(StorageLevel.MEMORY_AND_DISK)

            val targetRDD = SpatialReader.load(conf.target.path, conf.target.realIdField, conf.target.geometryField)
                .setName("TargetRDD").persist(StorageLevel.MEMORY_AND_DISK)
            (sourceRDD, targetRDD)
        }
        else {
            val targetRDD = SpatialReader.load(conf.target.path, conf.target.realIdField, conf.target.geometryField)
                .setName("TargetRDD").persist(StorageLevel.MEMORY_AND_DISK)

            val sourceRDD = SpatialReader.load(conf.source.path, conf.source.realIdField, conf.source.geometryField)
                .setName("SourceRDD").persist(StorageLevel.MEMORY_AND_DISK)
            (sourceRDD, targetRDD)
        }

        val distinctSourceMBB = sourceRDD.map(se => (se.originalID, se.mbb)).distinct().map(_._2).setName("distinctSourceMBB").cache()
        val sourceCount = distinctSourceMBB.count().toInt
        log.info("DS-JEDAI: Number of distinct profiles of Source: " + sourceCount + " in " + sourceRDD.getNumPartitions + " partitions")

        val distinctTargetMBB = targetRDD.map(se => (se.originalID, se.mbb)).distinct().map(_._2).setName("distinctTargetMBB").cache()
        val targetCount = distinctTargetMBB.count().toInt
        log.info("DS-JEDAI: Number of distinct profiles of Target: " + targetCount + " in " + targetRDD.getNumPartitions + " partitions")

        Utils(distinctSourceMBB, distinctTargetMBB, sourceCount, targetCount, conf.getTheta)
        val (source, target, relation) =
            if (Utils.toSwap) (targetRDD, sourceRDD, Relation.swap(conf.getRelation))
            else (sourceRDD, targetRDD, conf.getRelation)

        val matching_startTime = Calendar.getInstance().getTimeInMillis
        val matches = PartitionMatchingFactory.getMatchingAlgorithm(conf, source, target).apply(relation)

        log.info("DS-JEDAI: Matches: " + matches.count())
        val matching_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Matching Time: " + (matching_endTime - matching_startTime) / 1000.0)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }

}
