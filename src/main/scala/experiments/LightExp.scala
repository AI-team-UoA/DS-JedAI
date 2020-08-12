package experiments

import java.util.Calendar

import EntityMatching.LightAlgorithms.LightMatchingFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import utils.Readers.Reader
import utils.{ConfigurationParser, Utils}


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 *
 *         Execution:
 *         		spark-submit --master local[*] --class experiments.LightExp target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf <conf>
 *         Debug:
 *         		spark-submit --master local[*] --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 --class experiments.LightExp target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf <conf>
 */


object LightExp {

    def main(args: Array[String]): Unit = {
        val startTime = Calendar.getInstance().getTimeInMillis

        Logger.getLogger("org").setLevel(Level.INFO)
        Logger.getLogger("akka").setLevel(Level.INFO)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val sparkConf = new SparkConf()
            .setAppName("DS-JedAI")
            .set("spark.serializer", classOf[KryoSerializer].getName)
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

        val sourceRDD = Reader.read(conf.source.path, conf.source.realIdField, conf.source.geometryField, conf)
        val sourceCount = sourceRDD.setName("SourceRDD").cache().count()
        log.info("DS-JEDAI: Number of ptofiles of Source: " + sourceCount)

        // Loading Target
        val targetRDD = Reader.read(conf.target.path, conf.source.realIdField, conf.source.geometryField, conf)
        val targetCount = targetRDD.setName("TargetRDD").cache().count()
        log.info("DS-JEDAI: Number of ptofiles of Target: " + targetCount)

        Utils.targetCount = targetCount
        Utils.sourceCount = sourceCount

        val imRDD = LightMatchingFactory.getMatchingAlgorithm(conf, sourceRDD, targetRDD).applyDE9IM(0).persist(StorageLevel.MEMORY_AND_DISK)

        log.info("DS-JEDAI: CONTAINS: " + imRDD.filter(_.isContains).count())
        log.info("DS-JEDAI: COVERED BY: " + imRDD.filter(_.isCoveredBy).count())
        log.info("DS-JEDAI: COVERS: " + imRDD.filter(_.isCovers).count())
        log.info("DS-JEDAI: CROSSES: " + imRDD.filter(_.isCrosses).count())
        log.info("DS-JEDAI: EQUALS: " + imRDD.filter(_.isEquals).count())
        log.info("DS-JEDAI: INTERSECTS: " + imRDD.filter(_.isIntersects).count())
        log.info("DS-JEDAI: OVERLAPS: " + imRDD.filter(_.isOverlaps).count())
        log.info("DS-JEDAI: TOUCHES: " + imRDD.filter(_.isTouches).count())
        log.info("DS-JEDAI: WITHIN: " + imRDD.filter(_.isWithin).count())





        //log.info("DS-JEDAI: Matches: " + matches.count)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }
}
