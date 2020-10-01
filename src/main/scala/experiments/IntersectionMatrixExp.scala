package experiments

import java.util.Calendar

import EntityMatching.PartitionMatching.PartitionMatchingFactory
import EntityMatching.SpaceStatsCounter
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Constants.MatchingAlgorithm.MatchingAlgorithm
import utils.Constants.{MatchingAlgorithm, WeightStrategy}
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Readers.SpatialReader
import utils.{ConfigurationParser, Utils}

object IntersectionMatrixExp {

    def main(args: Array[String]): Unit = {
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
                case ("-f" | "-fraction") :: value :: tail =>
                    nextOption(map ++ Map("fraction" -> value), tail)
                case ("-s" | "-stats") :: tail =>
                    nextOption(map ++ Map("stats" -> "true"), tail)
                case "-auc" :: tail =>
                    nextOption(map ++ Map("auc" -> "true"), tail)
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
                case ("-b" | "-budget") :: value :: tail =>
                    nextOption(map ++ Map("budget" -> value), tail)
                case "-ws" :: value :: tail =>
                    nextOption(map ++ Map("ws" -> value), tail)
                case "-ma" :: value :: tail =>
                    nextOption(map ++ Map("ma" -> value), tail)
                case _ :: tail =>
                    log.warn("DS-JEDAI: Unrecognized argument")
                    nextOption(map, tail)
            }
        }

        val arglist = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), arglist)
        val stats = options.contains("stats")

        val sampleFraction = if (options.contains("fraction")) options("fraction").toDouble else -1d

        if (!options.contains("conf")) {
            log.error("DS-JEDAI: No configuration file!")
            System.exit(1)
        }

        val conf_path = options("conf")
        val conf = ConfigurationParser.parse(conf_path)
        val partitions: Int = if (options.contains("partitions")) options("partitions").toInt else conf.getPartitions

        val budget: Int = if (options.contains("budget")) options("budget").toInt else conf.getBudget
        val ws: String = if (options.contains("ws")) options("ws").toString else conf.getWeightingScheme.toString
        val ma: String = if (options.contains("ma")) options("ma").toString else conf.getMatchingAlgorithm.toString
        log.info("DS-JEDAI: Input Budget: " + budget)
        log.info("DS-JEDAI: Weighting Strategy: " + ws.toString)

        // setting SpatialReader
        SpatialReader.setPartitions(partitions)
        SpatialReader.noConsecutiveID()
        SpatialReader.setGridType(conf.getGridType)
        val startTime = Calendar.getInstance().getTimeInMillis

        val sourceRDD = SpatialReader.load(conf.source.path, conf.source.realIdField, conf.source.geometryField)
                        .setName("SourceRDD").persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = sourceRDD.count()//.map( _.originalID).countApproxDistinct()
        log.info("DS-JEDAI: Approximation of distinct profiles of Source: " + sourceCount + " in " + sourceRDD.getNumPartitions + " partitions")

        Utils(sourceRDD.map(_.mbb), sourceCount, conf.getTheta)
        val readTime = Calendar.getInstance()
        log.info("DS-JEDAI: Reading input dataset took: " + (readTime.getTimeInMillis - startTime) / 1000.0)

        val targetRDD = SpatialReader.load(conf.target.path, conf.target.realIdField, conf.target.geometryField)
        if(stats) SpaceStatsCounter(sourceRDD, targetRDD, conf.getTheta).printSpaceInfo()

        val de9im_startTime = Calendar.getInstance().getTimeInMillis
        if (!options.contains("auc")) {
            val pm = PartitionMatchingFactory.getMatchingAlgorithm(conf, sourceRDD, targetRDD, budget, ws, ma)
            val (totalContains, totalCoveredBy, totalCovers,totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin,intersectingPairs, interlinkedGeometries) = pm.countRelations

            val totalRelations = totalContains + totalCoveredBy + totalCovers + totalCrosses + totalEquals +
                totalIntersects + totalOverlaps + totalTouches + totalWithin
            log.info("DS-JEDAI: Total Intersecting Pairs: " + intersectingPairs)
            log.info("DS-JEDAI: Interlinked Geometries: " + interlinkedGeometries)
    
            log.info("DS-JEDAI: CONTAINS: " + totalContains)
            log.info("DS-JEDAI: COVERED BY: " + totalCoveredBy)
            log.info("DS-JEDAI: COVERS: " + totalCovers)
            log.info("DS-JEDAI: CROSSES: " + totalCrosses)
            log.info("DS-JEDAI: EQUALS: " + totalEquals)
            log.info("DS-JEDAI: INTERSECTS: " + totalIntersects)
            log.info("DS-JEDAI: OVERLAPS: " + totalOverlaps)
            log.info("DS-JEDAI: TOUCHES: " + totalTouches)
            log.info("DS-JEDAI: WITHIN: " + totalWithin)
            log.info("DS-JEDAI: Total Top Relations: " + totalRelations)
            val de9im_endTime = Calendar.getInstance().getTimeInMillis
            log.info("DS-JEDAI: Only DE-9IM Time: " + (de9im_endTime - de9im_startTime) / 1000.0)
        }
        else{
            val pm = PartitionMatchingFactory.getProgressiveAlgorithm(conf, sourceRDD, targetRDD, budget, ws, ma)
            var counter: Double = 0
            var auc: Double = 0
            var interlinkedGeometries: Double = 0
            pm.getWeightedDE9IM
                .map(p  => (p._1, p._2.relate))
                .takeOrdered(budget)(Ordering.by[(Double, Boolean), Double](_._1).reverse)
                .map(_._2)
                .foreach{ r =>
                   if (r) interlinkedGeometries += 1
                   auc += interlinkedGeometries
                   counter += 1
                }
            log.info("DS-JEDAI: Total Intersecting Pairs: " + counter)
            log.info("DS-JEDAI: Interlinked Geometries: " + interlinkedGeometries)
            log.info("DS-JEDAI: AUC: " + auc/interlinkedGeometries/counter)
        }

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }
}
