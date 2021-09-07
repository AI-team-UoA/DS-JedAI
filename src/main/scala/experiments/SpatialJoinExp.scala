package experiments

import linkers.DistributedInterlinking
import model.TileGranularities
import model.approximations.{GeometryApproximationT, GeometryToApproximation}
import model.entities.{EntityT, GeometryToEntity}
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

object SpatialJoinExp {
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
        log.info(s"G.I. Algorithm: GIA.nt")

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
        val output: Option[String] = conf.getOutputPath
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
        val sourceTransformer: Geometry => EntityT = GeometryToEntity.getTransformer(EntityTypeENUM.PREPARED_ENTITY, decompositionTheta, conf.source.datePattern, approximationTransformerOpt)
        val targetTransformer: Geometry => EntityT = GeometryToEntity.getTransformer(entityTypeType, decompositionTheta, conf.target.datePattern, approximationTransformerOpt)

        val sourceRDD: RDD[(Int, EntityT)] = partitioner.distributeAndTransform(sourceSpatialRDD, sourceTransformer)
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val targetRDD: RDD[(Int, EntityT)] = partitioner.distributeAndTransform(targetSpatialRDD, targetTransformer)
        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

        val partitionBorders = partitioner.getPartitionsBorders(theta)

        val matchingStartTime = Calendar.getInstance().getTimeInMillis
        val linkers = DistributedInterlinking.initializeLinkers(sourceRDD, targetRDD, partitionBorders, theta, partitioner)
        val matchingPairsRDD = DistributedInterlinking.relate(linkers, relation)
        val totalMatches = matchingPairsRDD.count()
        log.info("DS-JEDAI: " + relation.toString.toUpperCase() +": " + totalMatches)

        val matchingEndTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Interlinking Time: " + (matchingEndTime - matchingStartTime) / 1000.0)

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: Total Execution Time: " + (endTime - startTime) / 1000.0)
    }
}
