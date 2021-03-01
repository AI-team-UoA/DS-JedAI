package experiments


import dataModel.Entity
import geospatialInterlinking.GIAnt
import geospatialInterlinking.progressive.ProgressiveAlgorithmsFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Constants.ProgressiveAlgorithm.ProgressiveAlgorithm
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Constants.{GridType, ProgressiveAlgorithm, Relation, WeightingScheme}
import utils.{ConfigurationParser, SpatialReader, Utils}


object EvaluationExp {

    private val log: Logger = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    var budget: Int = 10000
    var takeBudget: Seq[Int] = Seq(5000000, 10000000)
    var relation: Relation = Relation.DE9IM

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val sparkConf = new SparkConf()
            .setAppName("DS-JedAI")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)

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
                case ("-b" | "-budget") :: value :: tail =>
                    nextOption(map ++ Map("budget" -> value), tail)
                case "-gt" :: value :: tail =>
                    nextOption(map ++ Map("gt" -> value), tail)
                case "-tv" :: value :: tail =>
                    nextOption(map ++ Map("tv" -> value), tail)
                case "-qp" :: value :: tail =>
                    nextOption(map ++ Map("qp" -> value), tail)
                case "-pa" :: value :: tail =>
                    nextOption(map ++ Map("pa" -> value), tail)
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
        val gridType: GridType.GridType = if (options.contains("gt")) GridType.withName(options("gt").toString) else conf.getGridType

        budget = if (options.contains("budget")) options("budget").toInt else conf.getBudget
        relation = conf.getRelation

        log.info("DS-JEDAI: Input Budget: " + budget)

        val reader = SpatialReader(conf.source, partitions, gridType)
        val sourceRDD = reader.load()
        sourceRDD.persist(StorageLevel.MEMORY_AND_DISK)
        Utils(sourceRDD.map(_._2.mbr), conf.getTheta, reader.partitionsZones)
        log.info(s"DS-JEDAI: Source was loaded into ${sourceRDD.getNumPartitions} partitions")

        val targetRDD = reader.load(conf.target)
        val partitioner = reader.partitioner

        val (totalVerifications, totalRelatedPairs) =
            if (options.contains("tv") && options.contains("qp"))
                (options("tv").toInt, options("qp").toInt)
            else {
                val g = GIAnt(sourceRDD, targetRDD, partitioner).countAllRelations
                (g._10, g._11)
            }

        log.info("DS-JEDAI: Total Verifications: " + totalVerifications)
        log.info("DS-JEDAI: Total Qualifying Pairs: " + totalRelatedPairs)
        log.info("\n")

        //printResults(sourceRDD, targetRDD, partitioner, totalRelatedPairs, ProgressiveAlgorithm.RANDOM,  (WeightingScheme.CF, None))

        val algorithms: Seq[ProgressiveAlgorithm] =
            if (options.contains("pa"))
                options("pa").split(",").filter(ProgressiveAlgorithm.exists).map(ProgressiveAlgorithm.withName).toSeq
            else
                Seq(ProgressiveAlgorithm.DYNAMIC_PROGRESSIVE_GIANT)

        val weightingSchemes = Seq((WeightingScheme.CF, None),
                                (WeightingScheme.JS, None),
                                (WeightingScheme.PEARSON_X2,None),
                                (WeightingScheme.MBR_INTERSECTION, None),
                                 (WeightingScheme.POINTS, None),
                                 (WeightingScheme.JS, Option(WeightingScheme.MBR_INTERSECTION)),
                                 (WeightingScheme.PEARSON_X2, Option(WeightingScheme.MBR_INTERSECTION)))

        for (a <- algorithms ; ws <- weightingSchemes)
            printResults(sourceRDD, targetRDD, partitioner, totalRelatedPairs, a, ws)
    }


    def printResults(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], partitioner: Partitioner, totalRelations: Int,
                     ma: ProgressiveAlgorithm, ws: (WeightingScheme, Option[WeightingScheme]), n: Int = 10): Unit = {

        val pma = ProgressiveAlgorithmsFactory.get(ma, source, target, partitioner, budget, ws._1, ws._2)
        val results = pma.evaluate(relation, n, totalRelations, takeBudget)

        results.zip(takeBudget).foreach { case ((pgr, qp, verifications, (verificationSteps, qualifiedPairsSteps)), b) =>
            val qualifiedPairsWithinBudget = if (totalRelations < verifications) totalRelations else verifications
            log.info(s"DS-JEDAI: ${ma.toString} Budget : $b")
            log.info(s"DS-JEDAI: ${ma.toString} Weighting Scheme: ${ws.toString}")
            log.info(s"DS-JEDAI: ${ma.toString} Total Verifications: $verifications")
            log.info(s"DS-JEDAI: ${ma.toString} Qualifying Pairs within budget: $qualifiedPairsWithinBudget")
            log.info(s"DS-JEDAI: ${ma.toString} Qualifying Pairs: $qp")
            log.info(s"DS-JEDAI: ${ma.toString} Recall: ${qp.toDouble / qualifiedPairsWithinBudget.toDouble}")
            log.info(s"DS-JEDAI: ${ma.toString} Precision: ${qp.toDouble / verifications.toDouble}")
            log.info(s"DS-JEDAI: ${ma.toString} PGR: $pgr")
            log.info(s"DS-JEDAI: ${ma.toString}: \nQualified Pairs\tVerified Pairs\n" + qualifiedPairsSteps.zip(verificationSteps)
                .map { case (qp: Int, vp: Int) => qp.toDouble / qualifiedPairsWithinBudget.toDouble + "\t" + vp.toDouble / verifications.toDouble }
                .mkString("\n"))
            log.info("\n")
        }
    }


    /**
     * compute Precision and PGR only for cases when bu > qp
     * @param bu budget
     * @param qp total qualifying pairs
     * @return PGR and qp
     */
    def computeOptimalMetrics(bu: Double, qp: Double): (Double, Double) ={
        val qpSum = (1d to qp by 1d).sum
        val rest = bu - qp
        val pgr = ((qpSum + (rest*qp))/qp.toDouble)/bu.toDouble
        val precision = qp.toDouble/bu.toDouble

        (pgr, precision)
    }

}
