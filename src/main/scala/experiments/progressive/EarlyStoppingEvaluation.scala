package experiments.progressive

import linkers.DistributedInterlinking
import linkers.progressive.DistributedProgressiveInterlinking
import model.TileGranularities
import model.entities.{EntityT, GeometryToEntity}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.ConfigurationParser
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants._
import utils.readers.{GridPartitioner, Reader}

/**
 * @author George Mandilaras (NKUA)
 */
object EarlyStoppingEvaluation {

	private val log: Logger = LogManager.getRootLogger
	log.setLevel(Level.INFO)

	// execution configuration
	val defaultBudget: Int = 300000000
	val takeBudget: Seq[Int] = Seq(300000000)
	val relation: Relation = Relation.DE9IM

	val BATCH_SIZES: Seq[Int] = Seq(500)
	val PRECISION_LIMITS: Seq[Float] = Seq(0.2f)
	val VIOLATIONS: Seq[Int] = Seq(3, 4, 5)

	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)

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
		val partitions: Int = conf.getPartitions
		val gridType: GridType.GridType = conf.getGridType
		val inputBudget = conf.getBudget
		log.info("DS-JEDAI: Input Budget: " + defaultBudget)
		// load datasets
		val sourceSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.source)
		val targetSpatialRDD: SpatialRDD[Geometry] = Reader.read(conf.target)
		val partitioner = GridPartitioner(sourceSpatialRDD, partitions, gridType)
		val approximateSourceCount = partitioner.approximateCount
		val theta = TileGranularities(sourceSpatialRDD.rawSpatialRDD.rdd.map(_.getEnvelopeInternal), approximateSourceCount, conf.getTheta)
		// spatial partition
		val geometry2entity: Geometry => EntityT = GeometryToEntity.getTransformer(EntityTypeENUM.SPATIAL_ENTITY, None, None)
		val sourceRDD: RDD[(Int, EntityT)] = partitioner.distributeAndTransform(sourceSpatialRDD, geometry2entity)
		val targetRDD: RDD[(Int, EntityT)] = partitioner.distributeAndTransform(targetSpatialRDD, geometry2entity)
		sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
		val partitionBorder = partitioner.getPartitionsBorders(theta)
		log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")
		log.info("\n")

		// to compute recall and precision we need overall results
		val (totalVerifications, totalQP) =
			(conf.getTotalVerifications, conf.getTotalQualifyingPairs) match {
				case (Some(tv), Some(qp)) =>
					(tv, qp)
				case _ =>
					val g = DistributedInterlinking.countAllRelations(DistributedInterlinking.initializeLinkers(sourceRDD, targetRDD, partitionBorder, theta, partitioner))
					(g._10, g._11)
			}

		val sourceCount = sourceSpatialRDD.rawSpatialRDD.count()
		val targetCount = targetSpatialRDD.rawSpatialRDD.count()
		val precision =(math floor(totalQP / (sourceCount*targetCount))  * 1000) / 1000
		log.info(s"-\t-\t-\t$totalQP\t$totalVerifications\t1.0\t$precision")

		val bestBatchSize = BATCH_SIZES.map{ batchSize =>
			val precision = evaluate(sourceRDD, targetRDD, theta, partitionBorder, approximateSourceCount, partitioner,
				totalQP, VIOLATIONS.head, PRECISION_LIMITS.head, batchSize )
			(precision, batchSize)
		}.maxBy(_._1)._2

		val bestPrecisionLimit = PRECISION_LIMITS.map{ precisionLimit =>
			val precision = evaluate(sourceRDD, targetRDD, theta, partitionBorder, approximateSourceCount, partitioner,
				totalQP, VIOLATIONS.head, precisionLimit, bestBatchSize )
			(precision, precisionLimit)
		}.maxBy(_._1)._2

		for (violations <- VIOLATIONS)
			evaluate(sourceRDD, targetRDD, theta, partitionBorder, approximateSourceCount, partitioner,
				totalQP, violations, bestPrecisionLimit, bestBatchSize)
	}


	def evaluate(source: RDD[(Int, EntityT)], target: RDD[(Int, EntityT)],
				 theta: TileGranularities, partitionBorders: Array[Envelope], sourceCount: Long,
				 partitioner: GridPartitioner, totalQualifiedPairs: Int, violation: Int, precisionLimit: Float, batchSize: Int ): Double = {

		val linkers = DistributedProgressiveInterlinking.initializeProgressiveLinkers(source, target,
			partitionBorders, theta, partitioner, ProgressiveAlgorithm.EARLY_STOPPING, defaultBudget, sourceCount,
			THIN_MULTI_COMPOSITE,WeightingFunction.JS, Option(WeightingFunction.CF), batchSize=batchSize,
			maxViolations=violation, precisionLevel=precisionLimit)
		val results = DistributedProgressiveInterlinking.evaluate(ProgressiveAlgorithm.EARLY_STOPPING, linkers, relation,
			totalQualifiedPairs = totalQualifiedPairs, takeBudget=takeBudget)
		val (_, qp, verifications, _) = results.head

		val qualifiedPairsWithinBudget = if (totalQualifiedPairs < verifications) totalQualifiedPairs else verifications
		val recall = (math floor(qp.toDouble / qualifiedPairsWithinBudget.toDouble)  * 1000) / 1000
		val precision =(math floor(qp.toDouble / verifications.toDouble)  * 1000) / 1000
		log.info(s"$batchSize\t$precisionLimit\t$violation\t$qp\t$verifications\t$recall\t$precision")
		precision
	}
}
