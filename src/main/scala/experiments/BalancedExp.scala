package experiments

import linkers.DistributedInterlinking
import linkers.loadbalancing.WellBalancedDistributedInterlinking
import model.TileGranularities
import model.approximations.{GeometryApproximationT, GeometryToApproximation}
import model.entities.{EntityT, GeometryToEntity, SpatialEntity}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import utils.configuration.ConfigurationParser
import utils.configuration.Constants.EntityTypeENUM.EntityTypeENUM
import utils.configuration.Constants.GeometryApproximationENUM.GeometryApproximationENUM
import utils.configuration.Constants.{EntityTypeENUM, GridType}
import utils.readers.{GridPartitioner, Reader}

import java.util.Calendar


object BalancedExp {

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

        log.info(s"G.I. Algorithm: Well Balancing - GIA.nt")

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
        val relation = conf.getRelation
        val measureTime = conf.measureStatistic
        val entityTypeType: EntityTypeENUM = conf.getEntityType
        val decompositionT: Option[Double] = conf.getDecompositionThreshold
        val startTime = Calendar.getInstance().getTimeInMillis

        log.info(s"GridType: $gridType")
        log.info(s"Relation: $relation")
        log.info(s"Entity Type: $entityTypeType")
        if(decompositionT.isDefined) log.info(s"Decomposition Threshold: ${decompositionT.get} ")

        // load datasets
        val sourceSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.source)
        val targetSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.target)

        // spatial partition
        val partitioner = GridPartitioner(sourceSpatialRDD, partitions, gridType)
        val approximateSourceCount = partitioner.approximateCount
        val theta = TileGranularities(sourceSpatialRDD.rawSpatialRDD.rdd.map(_.getEnvelopeInternal), approximateSourceCount, conf.getTheta)
        val decompositionTheta = decompositionT.map(dt => theta*dt)

        // set Approximation
        val approximationTypeOpt: Option[GeometryApproximationENUM] = conf.getApproximationType
        val approximationTransformerOpt: Option[Geometry => GeometryApproximationT] = GeometryToApproximation.getTransformer(approximationTypeOpt, decompositionTheta.getOrElse(theta))
        // set Entity type
        val sourceTransformer: Geometry => EntityT = GeometryToEntity.getTransformer(entityTypeType, theta, decompositionTheta, conf.source.datePattern, approximationTransformerOpt)
        val targetTransformer: Geometry => EntityT = GeometryToEntity.getTransformer(EntityTypeENUM.DECOMPOSED_ENTITY, theta, decompositionTheta, conf.target.datePattern, approximationTransformerOpt)

        val sourceRDD: RDD[(Int, EntityT)] = partitioner.distributeAndTransform(sourceSpatialRDD, sourceTransformer)
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val targetRDD: RDD[(Int, EntityT)] = partitioner.distributeAndTransform(targetSpatialRDD, targetTransformer)
        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

        val partitionBorders = partitioner.getPartitionsBorders(theta)


        if (!measureTime){
            val linkers = DistributedInterlinking.initializeLinkers(sourceRDD, targetRDD, partitionBorders, theta, partitioner)
            val verificationsRDD = WellBalancedDistributedInterlinking.batchedSegmentedVerificationRedistribution(linkers, theta)
            val imRDD = WellBalancedDistributedInterlinking.executeVerifications(verificationsRDD)

            val (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin, verifications, qp) = DistributedInterlinking.accumulateIM(imRDD)

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

            val endTime = Calendar.getInstance()
            log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
        }
        else {
            //DistributedInterlinking.executionStats(sourceRDD, targetRDD, partitionBorders, theta, partitioner)
            val linkers = DistributedInterlinking.initializeLinkers(sourceRDD, targetRDD, partitionBorders, theta, partitioner)
            WellBalancedDistributedInterlinking.timeBatchedSegmentedVerificationRedistribution(linkers, theta)
        }

        // TODO Remove
        System.in.read()
    }

}