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

object MultiExp {

    def main(args: Array[String]): Unit = {
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
                case ("-f" | "-fraction") :: value :: tail =>
                    nextOption(map ++ Map("fraction" -> value), tail)
                case ("-s" | "-stats") :: tail =>
                    nextOption(map ++ Map("stats" -> "true"), tail)
                case "-auc" :: tail =>
                    nextOption(map ++ Map("auc" -> "true"), tail)
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
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
        val partitions: Int = if (options.contains("partitions")) options("partitions").toInt else conf.getPartitions
        val startTime = Calendar.getInstance().getTimeInMillis

        val reader = SpatialReader(conf.source, partitions)
        val sourceRDD = reader.load2PartitionedRDD()
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        Utils(sourceRDD.map(_._2.mbb), conf.getTheta, reader.partitionsZones)

        val targetRDD = reader.load2PartitionedRDD(conf.target)
        val partitioner = reader.partitioner
        val readTime = Calendar.getInstance()
        val initialOverheard = (readTime.getTimeInMillis - startTime) / 1000.0

        val algorithms = Array("PROGRESSIVE_GIANT")
        val budgets = Array(10000000, 30000000, 50000000)
        val weightingSchemes = Array("JS", "CBS", "PEARSON_X2")

        for (ma <- algorithms; budget <- budgets; ws <- weightingSchemes) {
            log.info("DS-JEDAI: Input Budget: " + budget)
            log.info("DS-JEDAI: Weighting Strategy: " + ws)
            if (!options.contains("auc")) {
                val de9im_startTime = Calendar.getInstance().getTimeInMillis
                val pm = DMFactory.getProgressiveAlgorithm(conf, sourceRDD, targetRDD, partitioner, budget, ws, ma)
                val (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
                totalOverlaps, totalTouches, totalWithin, intersectingPairs, interlinkedGeometries) = pm.countRelations

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
                log.info("DS-JEDAI: Only DE-9IM Time: " + (((de9im_endTime - de9im_startTime) / 1000.0) + initialOverheard))
            }
            else {
                val pm = DMFactory.getProgressiveAlgorithm(conf, sourceRDD, targetRDD, partitioner, budget, ws, ma)
                var counter: Double = 0
                var auc: Double = 0
                var interlinkedGeometries: Double = 0
                pm.getWeightedDE9IM
                    .map(p => (p._1, p._2.relate))
                    .takeOrdered(budget)(Ordering.by[(Double, Boolean), Double](_._1).reverse)
                    .map(_._2)
                    .foreach { r =>
                        if (r) interlinkedGeometries += 1
                        auc += interlinkedGeometries
                        counter += 1
                    }
                log.info("DS-JEDAI: Total Intersecting Pairs: " + counter)
                log.info("DS-JEDAI: Interlinked Geometries: " + interlinkedGeometries)
                log.info("DS-JEDAI: AUC: " + auc / interlinkedGeometries / counter)
            }
        }
        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }
}

