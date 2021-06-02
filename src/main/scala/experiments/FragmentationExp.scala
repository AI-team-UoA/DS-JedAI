package experiments

import java.util.Calendar

import interlinkers.GIAnt
import model.TileGranularities
import model.entities.{Entity, IndexedFragmentedEntity}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import utils.Utils
import utils.configurationParser.ConfigurationParser
import utils.readers.{GridPartitioner, Reader}

object FragmentationExp {

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val sparkConf = new SparkConf()
            .setAppName("DS-JedAI")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)

        val sc = new SparkContext(sparkConf)
        val spark: SparkSession = SparkSession.builder().getOrCreate()

        // Parsing input arguments
        @scala.annotation.tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case ("-c" | "-conf") :: value :: tail =>
                    nextOption(map ++ Map("conf" -> value), tail)
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
                case _ :: tail =>
                    log.warn("DS-JEDAI: Unrecognized argument")
                    nextOption(map, tail)
            }
        }

        val argList = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), argList)

        if (!options.contains("conf")) {
            log.error("DS-JEDAI: No configuration file!")
            System.exit(1)
        }

        val confPath = options("conf")
        val conf = ConfigurationParser.parse(confPath)
        val partitions: Int = if (options.contains("partitions")) options("partitions").toInt else conf.getPartitions

        val startTime = Calendar.getInstance().getTimeInMillis

        val sourceSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.source)
        val targetSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.target)

        val partitioner = GridPartitioner(sourceSpatialRDD, partitions)
        val sourceRDD: RDD[(Int, Entity)] = partitioner.transform(sourceSpatialRDD, conf.source)
        val targetRDD: RDD[(Int, Entity)] = partitioner.transform(targetSpatialRDD, conf.target)
        val approximateSourceCount = partitioner.approximateCount
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)

        val theta = TileGranularities(sourceSpatialRDD.rawSpatialRDD.rdd.map(_.getEnvelopeInternal), approximateSourceCount, conf.getTheta)
        val partitionBorder = partitioner.getAdjustedPartitionsBorders(theta)

        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

//        spatial partition and fragmentation
//        val fragmentationF: Geometry => Seq[Geometry] = RecursiveFragmentation.splitBigGeometries(theta*4)
//        val fragmentedSourceRDD: RDD[(Int, Entity)] = sourceRDD.map(se => (se._1, FragmentedEntity(se._2)(fragmentationF)))
//        val fragmentedTargetRDD: RDD[(Int, Entity)] = targetRDD.map(se => (se._1, FragmentedEntity(se._2)(fragmentationF)))

        val fragmentedSourceRDD: RDD[(Int, Entity)] = sourceRDD.map(se => (se._1, IndexedFragmentedEntity(se._2, theta*2)))
        val fragmentedTargetRDD: RDD[(Int, Entity)] = targetRDD.map(se => (se._1, IndexedFragmentedEntity(se._2, theta*2)))

        val matchingStartTime = Calendar.getInstance().getTimeInMillis
        val giant = GIAnt(fragmentedSourceRDD, fragmentedTargetRDD, theta, partitionBorder, partitioner.hashPartitioner)
        val imRDD = giant.getDE9IM

        // log results
        val (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
        totalOverlaps, totalTouches, totalWithin, verifications, qp) = Utils.countAllRelations(imRDD)

        val totalRelations = totalContains + totalCoveredBy + totalCovers + totalCrosses + totalEquals +
            totalIntersects + totalOverlaps + totalTouches + totalWithin
        log.info("DS-JEDAI: Total Verifications: " + verifications)
        log.info("DS-JEDAI: Qualifying Pairs : " + qp)

        log.info("DS-JEDAI: CONTAINS: " + totalContains)
        log.info("DS-JEDAI: COVERED BY: " + totalCoveredBy)
        log.info("DS-JEDAI: COVERS: " + totalCovers)
        log.info("DS-JEDAI: CROSSES: " + totalCrosses)
        log.info("DS-JEDAI: EQUALS: " + totalEquals)
        log.info("DS-JEDAI: INTERSECTS: " + totalIntersects)
        log.info("DS-JEDAI: OVERLAPS: " + totalOverlaps)
        log.info("DS-JEDAI: TOUCHES: " + totalTouches)
        log.info("DS-JEDAI: WITHIN: " + totalWithin)
        log.info("DS-JEDAI: Total Discovered Relations: " + totalRelations)

        val matchingEndTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Interlinking Time: " + (matchingEndTime - matchingStartTime) / 1000.0)

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)
    }
}
