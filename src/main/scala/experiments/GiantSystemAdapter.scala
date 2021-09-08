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
import org.hobbit.core.components.AbstractSystemAdapter
import org.hobbit.core.rabbit.{RabbitMQUtils, SimpleFileReceiver}
import org.locationtech.jts.geom.Geometry
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.{EntityTypeENUM, GeometryApproximationENUM, Relation, ThetaOption}
import utils.configuration.DatasetConfigurations
import utils.readers.{GridPartitioner, Reader}

import java.nio.ByteBuffer
import java.util.Calendar
import scala.util.{Failure, Success, Try}


object GiantSystemAdapter extends AbstractSystemAdapter{

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val log: Logger = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    val sparkConf: SparkConf = new SparkConf()
        .setAppName("DS-JedAI")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    val sc: SparkContext = new SparkContext(sparkConf)
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    var sourceReceiver: SimpleFileReceiver = _
    var receivedGeneratedDataFilePath: String = _

    val geometryPredicate = "<http://strdf.di.uoa.gr/ontology#hasGeometry>"

    override def init(): Unit = {
        log.info("DS-JEDAI: Initializing GIA.nt test system")
        val initStartTime = Calendar.getInstance().getTimeInMillis
        super.init()
        log.info(s"DS-JEDAI: Super class initialized. It took ${Calendar.getInstance().getTimeInMillis - initStartTime}ms.")
        val receiverStartTime = Calendar.getInstance().getTimeInMillis

        sourceReceiver = SimpleFileReceiver.create(this.incomingDataQueueFactory, "source_file")
        log.info(s"DS-JEDAI: Source Receivers initialized. It took ${Calendar.getInstance().getTimeInMillis - receiverStartTime}ms.")
        log.info(s"DS-JEDAI: GIA.nt initialized successfully.")

    }

    override def receiveGeneratedData(data: Array[Byte]): Unit = {
        log.info("DS-JEDAI: Starting receiveGeneratedData")

        // read the file path
        val dataBuffer = ByteBuffer.wrap(data)

        // read the format of data
        val dataFormat = RabbitMQUtils.readString(dataBuffer)

         Try(RabbitMQUtils.readString(dataBuffer)) match {
            case Success(path) =>
                val receivedFiles = sourceReceiver.receiveData("./datasets/SourceDatasets/")
                receivedGeneratedDataFilePath = s"./datasets/SourceDatasets/${receivedFiles(0)}"
                log.info("DS-JEDAI: Received data from receiveGeneratedData")
            case Failure(ex) =>
                log.error(s"Error while receiving source Data. $ex")
        }
    }

    override def receiveGeneratedTask(taskId: String, data: Array[Byte]): Unit = {
        val startTime = Calendar.getInstance().getTimeInMillis

        val taskBuffer = ByteBuffer.wrap(data)

        // read the relation
        val taskRelationStr = RabbitMQUtils.readString(taskBuffer)
        val taskRelation = Relation.withName(taskRelationStr)
        log.info(s"taskRelation ${taskRelation.toString.toUpperCase}")

        // read the target geometry
        val targetGeom = RabbitMQUtils.readString(taskBuffer)
        log.info(s"targetGeom $targetGeom")

        // read namespace
        val namespace = RabbitMQUtils.readString(taskBuffer)
        log.info(s"namespace $namespace")

        // read the file path
        val taskFormat = RabbitMQUtils.readString(taskBuffer)
        log.info(s"Parsed task  $taskId. It took ${Calendar.getInstance().getTimeInMillis - startTime}ms.")

        val time = System.currentTimeMillis

        // warning: this should has been `org.hobbit.spatialbenchmark.rabbit.SingleFileReceiver`
        val targetReceiverTry = Try(SimpleFileReceiver.create(this.incomingDataQueueFactory, "task_target_file"))
        targetReceiverTry match {
            case Success(targetReceiver) =>
                val receivedFiles = targetReceiver.receiveData("./datasets/TargetDatasets/")
                val receivedGeneratedTaskFilePath = s"./datasets/TargetDatasets/${receivedFiles(0)}"

                // GIA.nt
                val taskResults = GiantController(receivedGeneratedDataFilePath, receivedGeneratedTaskFilePath, taskRelation)

                // send results
                val resultsArray = new Array[Array[Byte]](1)
                resultsArray(0) = taskResults.getBytes()
                val results = RabbitMQUtils.writeByteArrays(resultsArray)
                val resultSentRequest = Try(sendResultToEvalStorage(taskId, results))
                resultSentRequest match {
                    case Success(_) => log.info("Results sent to evaluation storage.")
                    case Failure(exception) => log.error(s"Exception while sending storage space cost to evaluation storage. $exception")
                }
            case Failure(exception) =>
            log.error(s"Exception while trying to receive data. Aborting. $exception")
        }
    }

    def GiantController(sourcePath: String, targetPath: String, relation: Relation): String = {
        val decompositionT: Option[Double] = Some(10)
        val partitions: Int = 10

        val sourceConf = DatasetConfigurations(sourcePath, geometryPredicate)
        val targetConf = DatasetConfigurations(targetPath, geometryPredicate)

        val sourceSpatialRDD: SpatialRDD[Geometry] = Reader.read(sourceConf)
        val targetSpatialRDD: SpatialRDD[Geometry] = Reader.read(targetConf)

        val partitioner = GridPartitioner(sourceSpatialRDD, partitions)
        val approximateSourceCount = partitioner.approximateCount
        val theta = TileGranularities(sourceSpatialRDD.rawSpatialRDD.rdd.map(_.getEnvelopeInternal), approximateSourceCount, ThetaOption.AVG)
        val decompositionTheta = decompositionT.map(dt => theta*dt)

        // set Approximation
        val approximationTransformerOpt: Option[Geometry => GeometryApproximationT] = GeometryToApproximation.getTransformer(Some(GeometryApproximationENUM.FINEGRAINED_ENVELOPES), decompositionTheta.getOrElse(theta))

        // set Entity type
        val sourceTransformer: Geometry => EntityT = GeometryToEntity.getTransformer(EntityTypeENUM.PREPARED_ENTITY, decompositionTheta, None, approximationTransformerOpt)
        val targetTransformer: Geometry => EntityT = GeometryToEntity.getTransformer(EntityTypeENUM.SPATIAL_ENTITY, decompositionTheta, None, approximationTransformerOpt)

        // spatial partitioning
        val sourceRDD: RDD[(Int, EntityT)] = partitioner.distributeAndTransform(sourceSpatialRDD, sourceTransformer)
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        val targetRDD: RDD[(Int, EntityT)] = partitioner.distributeAndTransform(targetSpatialRDD, targetTransformer)
        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")
        val partitionBorders = partitioner.getPartitionsBorders(theta)

        // start interlinking
        val linkers = DistributedInterlinking.initializeLinkers(sourceRDD, targetRDD, partitionBorders, theta, partitioner)
        val matchingPairsRDD = DistributedInterlinking.relate(linkers, relation)

        // transform pairs into the requested format
        matchingPairsRDD
            .mapPartitions { pairIterator =>
                val sb = new StringBuilder()
                pairIterator.foreach{case (s, t) => sb.append(s"$s\t$t\n")}
                Iterator(sb.toString())
            }
            .coalesce(numPartitions= 1)
            .collect()
            .mkString("")
    }

    override def close(): Unit = {
        log.info("Closing System Adapter...")
        // Always close the super class after yours!
        super.close()
        log.info("System Adapter closed successfully.")
    }
}
