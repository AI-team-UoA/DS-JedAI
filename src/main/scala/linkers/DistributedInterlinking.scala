package linkers

import cats.implicits._
import model.entities.segmented.DecomposedEntity
import model.entities.{Entity, SpatialEntity}
import model.{IM, IndicesPrefixTrie, TileGranularities}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.readers.GridPartitioner

import java.util.Calendar
import scala.collection.JavaConverters._
import scala.math.Numeric.IntIsIntegral

/**
 * Apply distributed Interlinking by initializing
 * a different linker in each partition
 */
object DistributedInterlinking {
    val BATCH_SIZE = 4096
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val log: Logger = LogManager.getRootLogger
    log.setLevel(Level.INFO)


    /**
     * Initialize a GIAnt Linker in each partition
     *
     * @param source source dataset
     * @param target target dataset
     * @param partitionBorders the borders of the partitions
     * @param theta tile granularities
     * @param gridPartitioner partitioner
     * @return an RDD with linkers in each partition
     */
    def initializeLinkers(source: RDD[(Int, Entity)], target: RDD[(Int, Entity)], partitionBorders: Array[Envelope],
                          theta: TileGranularities, gridPartitioner: GridPartitioner): RDD[LinkerT] = {

        val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = source.cogroup(target, gridPartitioner.hashPartitioner)
        joinedRDD.map { case (pid: Int, (sourceP: Iterable[Entity], targetP: Iterable[Entity])) =>
            val partition = partitionBorders(pid)
            GIAnt(sourceP.toArray, targetP, theta, partition)
        }
    }

    /**
     * Compute IM given an RDD of linkers
     * @param linkersRDD an RDD of Linkers
     * @return an RDD of IM
     */
    def computeIM(linkersRDD: RDD[LinkerT]): RDD[IM] =
        linkersRDD.flatMap(linker => linker.getDE9IM)

    def getTotalVerificationsPerPartition(linkersRDD: RDD[LinkerT]): RDD[Long] = linkersRDD.map(linker => linker.countVerification)

    /**
     * Compute the number of verifications provided an RDD of linkers
     * @param linkersRDD an RDD of linkers
     * @return the number of Verifications
     */
    def countVerifications(linkersRDD: RDD[LinkerT]): Double = getTotalVerificationsPerPartition(linkersRDD).sum

    /**
     * Find the pairs that satisfy the given relation
     * @param linkersRDD an RDD of linkers
     * @param relation a topological relation
     * @return an RDD of pairs that satisfy the given relation
     */
    def relate(linkersRDD: RDD[LinkerT], relation: utils.configuration.Constants.Relation.Relation): RDD[(String, String)] =
        linkersRDD.flatMap(linker => linker.relate(relation))

    /**
     * Compute all the relations given an RDD of linkers
     * @param linkersRDD an RDD of Linkers
     * @return the number of  all nine topological relations
     */
    def countAllRelations(linkersRDD: RDD[LinkerT]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = accumulateIM(computeIM(linkersRDD))

    /**
     * Accumulate the IM of an RDD of IM
     * @param imRDD an RDD of IM
     * @return the number of  all nine topological relations
     */
    def accumulateIM(imRDD: RDD[IM]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
        imRDD
            .mapPartitions { imIterator => Iterator(accumulate(imIterator)) }
            .treeReduce({ case (im1, im2) => im1 |+| im2 }, 4)

    val accumulate: Iterator[IM] => (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = imIterator => {
        var totalContains: Int = 0
        var totalCoveredBy: Int = 0
        var totalCovers: Int = 0
        var totalCrosses: Int = 0
        var totalEquals: Int = 0
        var totalIntersects: Int = 0
        var totalOverlaps: Int = 0
        var totalTouches: Int = 0
        var totalWithin: Int = 0
        var verifications: Int = 0
        var qualifiedPairs: Int = 0
        imIterator.foreach { im =>
            verifications += 1
            if (im.relate) {
                qualifiedPairs += 1
                if (im.isContains) totalContains += 1
                if (im.isCoveredBy) totalCoveredBy += 1
                if (im.isCovers) totalCovers += 1
                if (im.isCrosses) totalCrosses += 1
                if (im.isEquals) totalEquals += 1
                if (im.isIntersects) totalIntersects += 1
                if (im.isOverlaps) totalOverlaps += 1
                if (im.isTouches) totalTouches += 1
                if (im.isWithin) totalWithin += 1
            }
        }
        (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin, verifications, qualifiedPairs)
    }



    def executionStats(source: RDD[(Int, Entity)], target: RDD[(Int, Entity)], partitionBorders: Array[Envelope],
                       theta: TileGranularities, gridPartitioner: GridPartitioner): Unit ={

        val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))] = source.cogroup(target, gridPartitioner.hashPartitioner)
        val linkersRDD: RDD[(Iterator[GIAnt], Long)] = joinedRDD.mapPartitions { iter =>
            val startTime = Calendar.getInstance().getTimeInMillis
            val linkers = iter.map { case (pid: Int, (sourceP: Iterable[Entity], targetP: Iterable[Entity])) =>
                val partition = partitionBorders(pid)
                GIAnt(sourceP.toArray, targetP, theta, partition)
            }
            Iterator((linkers, startTime))
        }

        val timePerPartition = linkersRDD.mapPartitions{ linkerI =>
            val pid = TaskContext.getPartitionId()
            val time = linkerI.map { case (linkers, startTime) =>
                linkers.foreach(l => l.getDE9IM)
                val endTime = Calendar.getInstance().getTimeInMillis
                (endTime - startTime) / 1000.0
            }.max
            Iterator((pid, time))
        }.sortBy(_._1).collect()

        val verificationsPerPartition = linkersRDD.map { linkerI => linkerI._1.flatMap(l => l.getVerifications)}

        val basicStats = verificationsPerPartition.map{ verificationsI =>
            val verifications = verificationsI.toList
            val totalVerifications = verifications.size
            val (points, totalMax) = if (totalVerifications > 0) verifications.map { entities => (entities.head.geometry.getNumPoints, entities.tail.length)}.max else (0,0)
            (TaskContext.getPartitionId(), totalVerifications, points, totalMax)
        }.sortBy(_._1).collect()

        basicStats.zip(timePerPartition).foreach{ case( (pid, verifications, points, totalMax), (_, time)) =>
            log.info(pid+"\t"+verifications+"\t"+points+"\t"+totalMax+"\t"+time)}

    }

    def getOutliersRDD(linkersRDD: RDD[LinkerT], threshold: Double): (RDD[List[Entity]], RDD[List[Entity]]) ={

        // verifications accompanied of targets id
        val verificationsRDD = linkersRDD.flatMap { linker =>
            val verifications = linker.getVerifications
            verifications.map(entities => (entities, entities.head.originalID))
        }

        // approximate the cost of each target verifications
        val approximateCostOfTargetVerificationsRDD = verificationsRDD.map { case (verifications, id) =>
            val target = verifications.head
            val numPoints:Long = target.geometry.getNumPoints
            val totalVerifications:Long = verifications.tail.length
            ((numPoints*totalVerifications).toDouble, id)
        }.persist(StorageLevel.MEMORY_AND_DISK)

        val totalTargetVerifications = approximateCostOfTargetVerificationsRDD.count()
        val meanApproximateCost = approximateCostOfTargetVerificationsRDD.map(_._1).sum / totalTargetVerifications
        val variance = approximateCostOfTargetVerificationsRDD.map(x => math.pow(x._1 - meanApproximateCost, 2)).sum /totalTargetVerifications
        val std = Math.sqrt(variance)
        val zScore: Double => Double = (x: Double) => (x - meanApproximateCost)/std
        val outliersRDD = approximateCostOfTargetVerificationsRDD.map{case (cost, id) => (zScore(cost), id)}.filter(_._1 > threshold)
        val outliersID: Set[String] = outliersRDD.map(_._2).collect().toSet
        log.info(s"JEDAI: Total outliers ${outliersID.size}")

        val simpleVerificationsRDD = verificationsRDD.filter{ case(_, id) => !outliersID.contains(id)}.map(_._1)
        val expensiveVerificationsRDD = verificationsRDD.filter{ case(_, id) => outliersID.contains(id)}.map(_._1)

        approximateCostOfTargetVerificationsRDD.unpersist()

        (simpleVerificationsRDD, expensiveVerificationsRDD)
    }


    def batchedRedistribution(linkersRDD: RDD[LinkerT]): RDD[IM] ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = getOutliersRDD(linkersRDD, threshold = 3d)

        val wellBalancedImRDD = simpleVerificationsRDD
            .flatMap{ entities =>
                val t = entities.head
                val sourceEntities = entities.tail
                sourceEntities.map(s => s.getIntersectionMatrix(t) )
            }

        // WARNING: expensive and skewed repartitioning
        val overloadedImRDD = expensiveVerificationsRDD
            .flatMap{ entities =>
                val t = entities.head
                val sourceEntities = entities.tail
                sourceEntities.map(s => (s, t))
            }
            .repartition(linkersRDD.getNumPartitions)
            .map{ case (s, t) => s.getIntersectionMatrix(t)}

      wellBalancedImRDD.union(overloadedImRDD)

    }


    def segmentsVerificationRedistribution(linkersRDD: RDD[LinkerT]): RDD[IM] ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = getOutliersRDD(linkersRDD, threshold = 3d)

        val simpleImRDD = simpleVerificationsRDD
            .flatMap{ entities =>
                val t = entities.head
                val sourceEntities = entities.tail
                sourceEntities.map(s => s.getIntersectionMatrix(t) )
            }

        val overloadedImRDD = expensiveVerificationsRDD
            .flatMap { entities =>
                val t = entities.head.asInstanceOf[DecomposedEntity]
                val sourceEntities = entities.tail

                // for each source entity, find the indices of the segments it intersects
                val intersectingTargetSegments: Seq[(Entity, Seq[Int])] = sourceEntities.map(se => (se, t.findIntersectingSegmentsIndices(se).map(_._1)))

                // build trie to find which segments need to be verified with the same source geometries
                val trie = IndicesPrefixTrie(t.segments.length, intersectingTargetSegments)


                // extract from trie the verifications
                // create new entity using the segment as its geometry but maintaining its initial envelope
                // if a source entity needs to be verified with multiple segments, we unite them into a single geometry
                // WARNING: uniting non-intersecting geometries may lead to errors.
                trie.getFlattenNodes.map{ case (targetSegmentIndices, sourceEntities) =>
                    val targetSegments = targetSegmentIndices.map(i => t.segments(i)).asJava
                    val partialTarget = new UnaryUnionOp(targetSegments).union()
                    (SpatialEntity(t.originalID, partialTarget, t.env), sourceEntities)
                }
            }
            .repartition(linkersRDD.getNumPartitions)


//            val partitionsLoad = overloadedImRDD.mapPartitions(verificationsI => Iterator((TaskContext.getPartitionId(), verificationsI.map{ ver => ver._2.head.geometry.getNumPoints * ver._2.tail.size}.sum)))
//            log.info("Partition ID\tWeight")
//            partitionsLoad.sortBy(_._1).foreach{case (id, w) => log.info(id + "\t" + w)}

        simpleVerificationsRDD.union(overloadedImRDD)
    }

    def executeVerifications(verificationsRDD: RDD[List[Entity]]): RDD[IM] =
        verificationsRDD.flatMap{ entities =>
            val t = entities.head
            val sourceEntities = entities.tail
            sourceEntities.map(s => s.getIntersectionMatrix(t) )
        }

    def timeVerifications(verificationsRDD: RDD[List[Entity]]): Unit = {
        val timesRDD = verificationsRDD.mapPartitions { verificationsI =>
            val startTime = Calendar.getInstance().getTimeInMillis
            val pid = TaskContext.getPartitionId()
            val im = verificationsI.flatMap{ entities =>
                val t = entities.head
                val sourceEntities = entities.tail
                sourceEntities.map(s => s.getIntersectionMatrix(t) )
            }

            val endTime = Calendar.getInstance().getTimeInMillis
            val time = (endTime - startTime) / 1000.0
            Iterator((pid, time, im.length))
        }
        log.info("PID\tTime\t#Verifications")
        timesRDD.sortBy(_._1).foreach{case (pid, time, v) => log.info(pid + "\t" + time + "\t" + v)}
    }


    /***************************************************************************************************************/


    def getOutliersWithTimeRDD(linkersRDD: RDD[(Iterator[LinkerT], Long)], threshold: Double): (RDD[(Iterator[List[Entity]], Long)], RDD[(Iterator[List[Entity]], Long)]) = {

        // verifications accompanied of targets id
        val verificationsWithTimeRDD: RDD[(Iterator[List[(List[Entity], String)]], Long)] =
            linkersRDD.map { linkersI =>
                val startTime = linkersI._2
                val linkers = linkersI._1
                val verifications = linkers.map { linker =>
                    val verifications = linker.getVerifications.toList
                    verifications.map(entities => (entities, entities.head.originalID))
                }
                (verifications, startTime)
            }

        val verificationsRDD: RDD[Iterator[List[(List[Entity], String)]]] = verificationsWithTimeRDD.map(_._1)

        // approximate the cost of each target verifications
        val approximateCostOfTargetVerificationsRDD: RDD[(Double, String)] =
            verificationsRDD.flatMap { verificationsI =>
                verificationsI.flatMap { verificationsList =>
                    verificationsList.map { case (verifications, id) =>
                        val target = verifications.head
                        val numPoints: Long = target.geometry.getNumPoints
                        val totalVerifications: Long = verifications.tail.length
                        ((numPoints * totalVerifications).toDouble, id)
                    }
                }
            }.persist(StorageLevel.MEMORY_AND_DISK)

        val totalTargetVerifications = approximateCostOfTargetVerificationsRDD.count()
        val meanApproximateCost = approximateCostOfTargetVerificationsRDD.map(_._1).sum / totalTargetVerifications
        val variance = approximateCostOfTargetVerificationsRDD.map(x => math.pow(x._1 - meanApproximateCost, 2)).sum / totalTargetVerifications
        val std = Math.sqrt(variance)
        val zScore: Double => Double = (x: Double) => (x - meanApproximateCost) / std
        val outliersRDD = approximateCostOfTargetVerificationsRDD.map { case (cost, id) => (zScore(cost), id) }.filter(_._1 > threshold)
        val outliersID: Set[String] = outliersRDD.map(_._2).collect().toSet
        log.info(s"JEDAI: Total outliers ${outliersID.size}")

        val simpleVerificationsRDD: RDD[(Iterator[List[Entity]], Long)] =
            verificationsWithTimeRDD.map { verificationsI =>
                val startTime = verificationsI._2
                val filteredVerifications: Iterator[List[Entity]] = verificationsI._1.flatMap { verificationsList =>
                    verificationsList.filter { case (_, id) => !outliersID.contains(id)}.map(_._1)
                }
                (filteredVerifications, startTime)
            }

        simpleImRDD.union(overloadedImRDD)
    }
}
