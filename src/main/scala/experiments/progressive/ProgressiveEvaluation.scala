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
import utils.configuration.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction
import utils.configuration.Constants._
import utils.configuration.{ConfigurationParser, Constants}
import utils.readers.{GridPartitioner, Reader}

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object ProgressiveEvaluation {

	private val log: Logger = LogManager.getRootLogger
	log.setLevel(Level.INFO)

	// execution configuration
	val defaultBudget: Int = 3000
	val takeBudget: Seq[Int] = Seq(20000000)
	val relation: Relation = Relation.DE9IM
	val weightingSchemes: Seq[Constants.WeightingScheme] = Seq(HYBRID)
	val weightingFunctions: Seq[(WeightingFunction, Option[WeightingFunction])] = Seq((WeightingFunction.PEARSON_X2, Some(WeightingFunction.MBRO)))
	val selectedAlgorithms = Seq(ProgressiveAlgorithm.PROGRESSIVE_GIANT, ProgressiveAlgorithm.EARLY_STOPPING)

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
		val budget = if (inputBudget > 0) inputBudget else defaultBudget
		log.info("DS-JEDAI: Input Budget: " + budget)
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

		// to compute recall and precision we need overall results
		val (totalVerifications, totalRelatedPairs) =
			(conf.getTotalVerifications, conf.getTotalQualifyingPairs) match {
				case (Some(tv), Some(qp)) =>
					(tv, qp)
				case _ =>
					val g = DistributedInterlinking.countAllRelations(DistributedInterlinking.initializeLinkers(sourceRDD, targetRDD, partitionBorder, theta, partitioner))
					(g._10, g._11)
			}
		log.info("DS-JEDAI: Total Verifications: " + totalVerifications)
		log.info("DS-JEDAI: Total Qualifying Pairs: " + totalRelatedPairs)
		log.info("\n")

		//        printResults(sourceRDD, targetRDD, theta, partitionBorder, approximateSourceCount, partitioner,
		//            totalRelatedPairs, budget, ProgressiveAlgorithm.RANDOM,  (WeightingFunction.CF, None), Constants.SIMPLE)

		for (a <- selectedAlgorithms; ws <- weightingSchemes; wf <- weightingFunctions)
			printResults(sourceRDD, targetRDD, theta, partitionBorder, approximateSourceCount, partitioner,
				totalRelatedPairs, budget, a, wf, ws)
	}


	/**
	 * Execute and Evaluate the selected algorithm with the selected configuration
	 *
	 * @param source         source entities
	 * @param target         target entities
	 * @param partitioner    the partitioner
	 * @param totalRelations the target relations
	 * @param pa             the progressive algorithms
	 * @param wf             the weighting function
	 * @param ws             the weighting scheme
	 * @param n              the size of list storing the results
	 */
	def printResults(source: RDD[(Int, EntityT)], target: RDD[(Int, EntityT)],
					 theta: TileGranularities, partitionBorders: Array[Envelope], sourceCount: Long,
					 partitioner: GridPartitioner, totalRelations: Int, budget: Int,
					 pa: ProgressiveAlgorithm, wf: (WeightingFunction, Option[WeightingFunction]), ws: Constants.WeightingScheme, n: Int = 10): Unit = {

		val linkers = DistributedProgressiveInterlinking.initializeProgressiveLinkers(source, target,
			partitionBorders, theta, partitioner, pa, budget, sourceCount, ws, wf._1, wf._2)
		val results = DistributedProgressiveInterlinking.evaluate(pa, linkers, relation, n, totalRelations, takeBudget)

		results.zip(takeBudget).foreach { case ((pgr, qp, verifications, (verificationSteps, qualifiedPairsSteps)), b) =>
			val qualifiedPairsWithinBudget = if (totalRelations < verifications) totalRelations else verifications
			log.info(s"DS-JEDAI: ${pa.toString} Budget : $b")
			log.info(s"DS-JEDAI: ${pa.toString} Weighting Scheme: ${ws.value}")
			log.info(s"DS-JEDAI: ${pa.toString} Weighting Function: ${wf.toString}")
			log.info(s"DS-JEDAI: ${pa.toString} Total Verifications: $verifications")
			log.info(s"DS-JEDAI: ${pa.toString} Qualifying Pairs within budget: $qualifiedPairsWithinBudget")
			log.info(s"DS-JEDAI: ${pa.toString} Detected Qualifying Pairs: $qp")
			log.info(s"DS-JEDAI: ${pa.toString} Recall: ${qp.toDouble / qualifiedPairsWithinBudget.toDouble}")
			log.info(s"DS-JEDAI: ${pa.toString} Precision: ${qp.toDouble / verifications.toDouble}")
			log.info(s"DS-JEDAI: ${pa.toString} PGR: $pgr")

			//            log.info(s"DS-JEDAI: ${pa.toString}: \nQualified Pairs\tVerified Pairs\n" + qualifiedPairsSteps.zip(verificationSteps)
			//                .map { case (qp: Int, vp: Int) => qp.toDouble / qualifiedPairsWithinBudget.toDouble + "\t" + vp.toDouble / verifications.toDouble }
			//                .mkString("\n"))
			log.info("\n")
		}
	}


	/**
	 * compute Precision and PGR only for cases when bu > qp
	 *
	 * @param bu budget
	 * @param qp total qualifying pairs
	 * @return PGR and qp
	 */
	def computeOptimalMetrics(bu: Double, qp: Double): (Double, Double) = {
		val qpSum = BigDecimal(1d).until(qp, 1d).sum
		val rest = bu - qp
		val pgr = ((qpSum + (rest * qp)) / qp) / bu
		val precision = qp / bu

		(pgr.toDouble, precision)
	}

}
