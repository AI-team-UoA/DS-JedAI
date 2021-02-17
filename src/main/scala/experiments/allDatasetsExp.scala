package experiments


import DataStructures.Entity
import EntityMatching.DistributedMatching.{DMFactory, GIAnt}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Constants.MatchingAlgorithm.MatchingAlgorithm
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Constants.{GridType, MatchingAlgorithm, Relation, WeightStrategy}
import utils.{ConfigurationParser, SpatialReader, Utils}


object allDatasetsExp {

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
                case "-ws" :: value :: tail =>
                    nextOption(map ++ Map("ws" -> value), tail)
                case "-gt" :: value :: tail =>
                    nextOption(map ++ Map("gt" -> value), tail)
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

        val (_, _, _, _, _, _, _, _, _, totalVerifications, totalRelatedPairs) = GIAnt(sourceRDD, targetRDD, WeightStrategy.JS, budget, partitioner).countAllRelations

        log.info("DS-JEDAI: Total Verifications: " + totalVerifications)
        log.info("DS-JEDAI: Total Interlinked Geometries: " + totalRelatedPairs)
        log.info("\n")

        printResults(sourceRDD, targetRDD, partitioner, totalRelatedPairs, MatchingAlgorithm.GIANT,  WeightStrategy.CBS)
        val algorithms = Seq(MatchingAlgorithm.PROGRESSIVE_GIANT, MatchingAlgorithm.TOPK, MatchingAlgorithm.RECIPROCAL_TOPK, MatchingAlgorithm.GEOMETRY_CENTRIC)
        val weightingSchemes = Seq(WeightStrategy.MBR_INTERSECTION, WeightStrategy.POINTS)
        for (a <- algorithms ; ws <- weightingSchemes)
            printResults(sourceRDD, targetRDD, partitioner, totalRelatedPairs, a, ws)
    }


    def printResults(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], partitioner: Partitioner, totalRelations: Int,
                    ma: MatchingAlgorithm, ws: WeightStrategy, n: Int = 10): Unit = {

        val pma = DMFactory.getMatchingAlgorithm(ma, source, target, partitioner, budget, ws)
        val results = pma.getAUC(relation, n, totalRelations, takeBudget)

        results.zip(takeBudget).foreach { case ((pgr, interlinkedGeometries, totalVerifications, (verifications, qualifiedPairs)), b) =>
            val qualifiedPairsWithinBudget = if (totalRelations < totalVerifications) totalRelations else totalVerifications
            log.info(s"DS-JEDAI: ${ma.toString} Budget : $b")
            log.info(s"DS-JEDAI: ${ma.toString} Weighting Scheme: ${ws.toString}")
            log.info(s"DS-JEDAI: ${ma.toString} Total Verifications: $totalVerifications")
            log.info(s"DS-JEDAI: ${ma.toString} Qualifying Pairs within budget: $qualifiedPairsWithinBudget")
            log.info(s"DS-JEDAI: ${ma.toString} Interlinked Geometries: $interlinkedGeometries")
            log.info(s"DS-JEDAI: ${ma.toString} Recall: ${interlinkedGeometries.toDouble / qualifiedPairsWithinBudget.toDouble}")
            log.info(s"DS-JEDAI: ${ma.toString} Precision: ${interlinkedGeometries.toDouble / totalVerifications.toDouble}")
            log.info(s"DS-JEDAI: ${ma.toString} PGR(R): $pgr")
            log.info(s"DS-JEDAI: ${ma.toString}: \nQualified Pairs\tVerified Pairs\n" + qualifiedPairs.zip(verifications)
                .map { case (qp: Int, vp: Int) => qp.toDouble / qualifiedPairsWithinBudget.toDouble + "\t" + vp.toDouble / totalVerifications.toDouble }
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
