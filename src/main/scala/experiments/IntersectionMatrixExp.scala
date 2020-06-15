package experiments

import java.util.Calendar

import EntityMatching.PartitionMatching.PartitionMatching
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Readers.SpatialReader
import utils.{ConfigurationParser, Utils}

object IntersectionMatrixExp {
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
        val sourceRDD = SpatialReader.load(conf.source.path, conf.source.realIdField, conf.source.geometryField)
            .setName("SourceRDD").persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = sourceRDD.count().toInt
        log.info("DS-JEDAI: Number of profiles of Source: " + sourceCount + " in " + sourceRDD.getNumPartitions + " partitions")

        // Loading Target
        val targetRDD = SpatialReader.load(conf.target.path, conf.source.realIdField, conf.source.geometryField)
            .setName("TargetRDD").persist(StorageLevel.MEMORY_AND_DISK)
        val targetCount = targetRDD.count().toInt
        log.info("DS-JEDAI: Number of profiles of Target: " + targetCount + " in " + targetRDD.getNumPartitions + " partitions")

        val (source, target, _) = Utils.swappingStrategy(sourceRDD, targetRDD, conf.getRelation, sourceCount, targetCount)

        val matching_startTime = Calendar.getInstance().getTimeInMillis
        val imRDD = PartitionMatching(source, target, conf.getTheta).getDE9IM
            .setName("IntersectionMatrixRDD").persist(StorageLevel.MEMORY_AND_DISK)

        log.info("CONTAINS: " + imRDD.filter(_.isContains).count())
        log.info("COVERED BY: " + imRDD.filter(_.isCoveredBy).count())
        log.info("COVERS: " + imRDD.filter(_.isCovers).count())
        log.info("CROSSES: " + imRDD.filter(_.isCrosses).count())
        log.info("EQUALS: " + imRDD.filter(_.isEquals).count())
        log.info("INTERSECTS: " + imRDD.filter(_.isIntersects).count())
        log.info("OVERLAPS: " + imRDD.filter(_.isOverlaps).count())
        log.info("TOUCHES: " + imRDD.filter(_.isTouches).count())
        log.info("WITHIN: " + imRDD.filter(_.isWithin).count())

        val matching_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: DE-9IM Time: " + (matching_endTime - matching_startTime) / 1000.0)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }
}
