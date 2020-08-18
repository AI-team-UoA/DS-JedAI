package experiments

import java.util.Calendar

import EntityMatching.PartitionMatching.{PartitionMatching, PartitionMatchingFactory}
import EntityMatching.SpaceStatsCounter
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import utils.Constants.MatchingAlgorithm
import utils.Readers.SpatialReader
import utils.{Configuration, ConfigurationParser, Utils}

object IntersectionMatrixExp {
    def main(args: Array[String]): Unit = {
        val startTime = Calendar.getInstance().getTimeInMillis
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val sparkConf = new SparkConf()
            .setAppName("DS-JedAI")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = new SparkContext(sparkConf)
        val spark: SparkSession = SparkSession.builder().getOrCreate()

        // Parsing the input arguments
        @scala.annotation.tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case ("-c" | "-conf") :: value :: tail =>
                    nextOption(map ++ Map("conf" -> value), tail)
                case _ :: tail =>
                    log.warn("DS-JEDAI: Unrecognized argument")
                    nextOption(map, tail)
            }
        }

        val arglist = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), arglist)

        if (!options.contains("conf")) {
            log.error("DS-JEDAI: No configuration file!")
            System.exit(1)
        }

        val conf_path = options("conf")
        val conf = ConfigurationParser.parse(conf_path)
        val partitions: Int = conf.getPartitions

        // Loading Source
        SpatialReader.setPartitions(partitions)
        SpatialReader.noConsecutiveID()
        SpatialReader.setGridType(conf.getGridType)
        val sourceRDD = SpatialReader.load(conf.source.path, conf.source.realIdField, conf.source.geometryField)
            .setName("SourceRDD").persist(StorageLevel.MEMORY_AND_DISK)
        val sourceCount = sourceRDD.map(_.originalID).distinct().count().toInt
        log.info("DS-JEDAI: Number of profiles of Source: " + sourceCount + " in " + sourceRDD.getNumPartitions + " partitions")

        // Loading Target
        val targetRDD = SpatialReader.load(conf.target.path, conf.source.realIdField, conf.source.geometryField)
            .setName("TargetRDD").persist(StorageLevel.MEMORY_AND_DISK)
        val targetCount = targetRDD.map(_.originalID).distinct().count().toInt
        log.info("DS-JEDAI: Number of profiles of Target: " + targetCount + " in " + targetRDD.getNumPartitions + " partitions")

        val (source, target, _) = Utils.swappingStrategy(sourceRDD, targetRDD, conf.getRelation, sourceCount, targetCount)

        val matching_startTime = Calendar.getInstance().getTimeInMillis

        val ma = conf.getMatchingAlgorithm
        if (ma == MatchingAlgorithm.SPATIAL) {

            SpaceStatsCounter(sourceRDD, targetRDD, conf.getTheta).printSpaceInfo()

            val pm = PartitionMatching(source, target, conf.getTheta)
            val imRDD = pm.getDE9IM
                .setName("IntersectionMatrixRDD").persist(StorageLevel.MEMORY_AND_DISK)

            log.info("DS-JEDAI: CONTAINS: " + imRDD.filter(_.isContains).count())
            log.info("DS-JEDAI: COVERED BY: " + imRDD.filter(_.isCoveredBy).count())
            log.info("DS-JEDAI: COVERS: " + imRDD.filter(_.isCovers).count())
            log.info("DS-JEDAI: CROSSES: " + imRDD.filter(_.isCrosses).count())
            log.info("DS-JEDAI: EQUALS: " + imRDD.filter(_.isEquals).count())
            log.info("DS-JEDAI: INTERSECTS: " + imRDD.filter(_.isIntersects).count())
            log.info("DS-JEDAI: OVERLAPS: " + imRDD.filter(_.isOverlaps).count())
            log.info("DS-JEDAI: TOUCHES: " + imRDD.filter(_.isTouches).count())
            log.info("DS-JEDAI: WITHIN: " + imRDD.filter(_.isWithin).count())
        }
        else{
            val IMsIter = PartitionMatchingFactory.getProgressiveAlgorithm(conf: Configuration, source, target).getDE9IMBudget
            var detectedLinks = 0
            var interlinkedGeometries = 0


            var containsArray = Array[(String, String)]()
            var coveredByArray = Array[(String, String)]()
            var coversArray = Array[(String, String)]()
            var crossesArray = Array[(String, String)]()
            var equalsArray = Array[(String, String)]()
            var intersectsArray = Array[(String, String)]()
            var overlapsArray = Array[(String, String)]()
            var touchesArray = Array[(String, String)]()
            var withinArray = Array[(String, String)]()


            var i:Int = 0
            IMsIter
                .foreach(im => {
                    var relate = false
                    if (i % 10000 == 0)
                        log.info("DS-JEDAI: Iteration: " + i +" Links\t:\t" + interlinkedGeometries + "\t" + detectedLinks )

                    if (im.isContains) {
                        relate = true
                        detectedLinks += 1
                        containsArray = containsArray :+ im.idPair
                    }

                    if (im.isCoveredBy) {
                        relate = true
                        detectedLinks += 1
                        coveredByArray = coveredByArray :+ im.idPair
                    }

                    if (im.isCovers) {
                        relate = true
                        detectedLinks += 1
                        coversArray = coversArray :+ im.idPair
                    }

                    if (im.isCrosses) {
                        relate = true
                        detectedLinks += 1
                        crossesArray = crossesArray :+ im.idPair
                    }

                    if (im.isEquals) {
                        relate = true
                        detectedLinks += 1
                        equalsArray = equalsArray :+ im.idPair
                    }

                    if (im.isIntersects) {
                        relate = true
                        detectedLinks += 1
                        intersectsArray = intersectsArray :+ im.idPair
                    }

                    if (im.isOverlaps) {
                        relate = true
                        detectedLinks += 1
                        overlapsArray = overlapsArray :+ im.idPair
                    }

                    if (im.isTouches) {
                        relate = true
                        detectedLinks += 1
                        touchesArray = touchesArray :+ im.idPair
                    }

                    if (im.isWithin) {
                        relate = true
                        detectedLinks += 1
                        withinArray = withinArray :+ im.idPair
                    }

                    if (relate)
                        interlinkedGeometries += 1
                    i += 1
                })
            log.info("DS-JEDAI: Iteration: " + i +" Links\t:\t" + interlinkedGeometries + "\t" + detectedLinks )
            log.info("\n")
            log.info("DS-JEDAI: CONTAINS: " + containsArray.length)
            log.info("DS-JEDAI: COVERED BY: " + coveredByArray.length)
            log.info("DS-JEDAI: COVERS: " + coversArray.length)
            log.info("DS-JEDAI: CROSSES: " + crossesArray.length)
            log.info("DS-JEDAI: EQUALS: " + equalsArray.length)
            log.info("DS-JEDAI: INTERSECTS: " + intersectsArray.length)
            log.info("DS-JEDAI: OVERLAPS: " + overlapsArray.length)
            log.info("DS-JEDAI: TOUCHES: " + touchesArray.length)
            log.info("DS-JEDAI: WITHIN: " + withinArray.length + "\n")

        }

        val matching_endTime = Calendar.getInstance().getTimeInMillis
        log.info("DS-JEDAI: DE-9IM Time: " + (matching_endTime - matching_startTime) / 1000.0)

        val endTime = Calendar.getInstance()
        log.info("DS-JEDAI: Total Execution Time: " + (endTime.getTimeInMillis - startTime) / 1000.0)
    }
}
