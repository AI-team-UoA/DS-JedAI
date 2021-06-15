package experiments

import java.util.Calendar

import interlinkers.DirtyGIAnt
import model.TileGranularities
import model.entities.{Entity, SpatialEntityType}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import utils.configuration.Constants.GridType
import utils.Utils
import utils.configuration.ConfigurationParser
import utils.readers.{GridPartitioner, Reader}

object DirtyExp {

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
        val parser = new ConfigurationParser()
        val configurationOpt = parser.parse(args) match {
            case Left(errors) =>
                errors.foreach(e => log.error(e.getMessage))
                System.exit(1)
                None
            case Right(configuration) => Some(configuration)
        }
        val conf = configurationOpt.get

        val partitions: Int = conf.getPartitions
        val gridType: GridType.GridType = conf.getGridType
        val output: Option[String] = conf.getOutputPath
        val print = conf.measureStatistic

        val startTime = Calendar.getInstance().getTimeInMillis

        // load datasets
        val sourceSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.source)

        // spatial partition
        val entityType = SpatialEntityType()
        val partitioner = GridPartitioner(sourceSpatialRDD, partitions, gridType)
        val sourceRDD: RDD[(Int, Entity)] = partitioner.distributeAndTransform(sourceSpatialRDD, entityType)
        val approximateSourceCount = partitioner.approximateCount
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)

        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

        val theta = TileGranularities(sourceRDD.map(_._2.env), approximateSourceCount, conf.getTheta)
        val partitionBorder = partitioner.getAdjustedPartitionsBorders(theta)
        val giant = DirtyGIAnt(sourceRDD.map(_._2), partitionBorder, theta)
        val imRDD = giant.getDE9IM

        if (print) {
            imRDD.persist(StorageLevel.MEMORY_AND_DISK)
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
        }
        if(output.isDefined) Utils.exportRDF(imRDD, output.get)
        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)
    }
}