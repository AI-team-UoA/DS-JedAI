package experiments

import model.TileGranularities
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.Geometry
import utils.configuration.ConfigurationParser
import utils.configuration.Constants.EntityTypeENUM.EntityTypeENUM
import utils.configuration.Constants.GridType
import utils.readers.{GridPartitioner, Reader}

import java.util.Calendar

object MeasureStats {

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

        val parser = new ConfigurationParser()
        val configurationOpt = parser.parse(args) match {
            case Left(errors) =>
                errors.foreach(e => log.error(e.getMessage))
                System.exit(1)
                None
            case Right(configuration) => Some(configuration)
        }
        val conf = configurationOpt.get
        conf.print(log)
        val partitions: Int = conf.getPartitions
        val gridType: GridType.GridType = conf.getGridType
        val entityTypeType: EntityTypeENUM = conf.getEntityType
        val decompositionT: Option[Double] = conf.getDecompositionThreshold
        val startTime = Calendar.getInstance().getTimeInMillis

        // load datasets
        val sourceSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.source)
        sourceSpatialRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = sourceSpatialRDD.rawSpatialRDD.count()


        val targetSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.target)
        targetSpatialRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val targetCount = targetSpatialRDD.rawSpatialRDD.count()

        // spatial partition
        val partitioner = GridPartitioner(sourceSpatialRDD, partitions, gridType)
        val approximateSourceCount = partitioner.approximateCount
        val theta = TileGranularities(sourceSpatialRDD.rawSpatialRDD.rdd.map(_.getEnvelopeInternal), approximateSourceCount, conf.getTheta)
        val decompositionTheta = theta * decompositionT.getOrElse(10)

        log.info(s"AVG AREA: ${theta.x * theta.y}")
        val fgeSource = sourceSpatialRDD.rawSpatialRDD.rdd.map(g => g.getEnvelopeInternal)
            .filter(e => e.getWidth > decompositionTheta.x || e.getHeight > decompositionTheta.y).count()
        val fgeTarget = targetSpatialRDD.rawSpatialRDD.rdd.map(g => g.getEnvelopeInternal)
            .filter(e => e.getWidth > decompositionTheta.x || e.getHeight > decompositionTheta.y).count()

        log.info(s"Source FGE Geometries: $fgeSource")
        log.info(s"Source FGE Geometries Percentage: ${(fgeSource.toDouble/sourceCount.toDouble)*100}")
        log.info(s"Target FGE Geometries: $fgeTarget")
        log.info(s"Target FGE Geometries Percentage: ${(fgeTarget.toDouble/targetCount.toDouble)*100}")
    }

}
