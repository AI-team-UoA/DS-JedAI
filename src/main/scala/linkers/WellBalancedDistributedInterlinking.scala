package linkers

import linkers.DistributedInterlinking.log
import model.entities.segmented.DecomposedEntity
import model.entities.{EntityT, SpatialEntity}
import model.{IM, TileGranularities}
import model.structures.IndicesPrefixTrie
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.operation.union.UnaryUnionOp

import java.util.Calendar
import scala.collection.JavaConverters._

object WellBalancedDistributedInterlinking {

    /**
     *  Partition verifications into two groups, skew and cheap
     *  For each verification, i.e., RDD[t::S_n], estimate its weight as W_t = t.getNumPoints*|S_n|
     *  consider the verifications that their z-score > threshold as skew and the rest as cheap ones
     *
     * @param linkersRDD an RDD of linkers
     * @param threshold a z-score threshold
     * @return a tuple containing the skew and cheap verifications as RDDs
     */
    def partitionVerifications(linkersRDD: RDD[LinkerT], threshold: Double=3d): (RDD[List[EntityT]], RDD[List[EntityT]]) ={

        // verifications accompanied of targets id
        val verificationsRDD: RDD[(List[EntityT], String)] = linkersRDD.flatMap { linker =>
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

        // partition based on z-score  and threshold
        val skewVerificationsCostRDD: RDD[(Double, String)] = approximateCostOfTargetVerificationsRDD.map{case (cost, id) => (zScore(cost), id)}.filter(_._1 > threshold)
        val skewVerificationsIDs: Set[String] = skewVerificationsCostRDD.map(_._2).collect().toSet
        log.info(s"JEDAI: Total skew verifications ${skewVerificationsIDs.size}")

        val cheapVerificationsRDD: RDD[List[EntityT]] = verificationsRDD.filter{ case(_, id) => !skewVerificationsIDs.contains(id)}.map(_._1)
        val skewVerificationsRDD: RDD[List[EntityT]] = verificationsRDD.filter{ case(_, id) => skewVerificationsIDs.contains(id)}.map(_._1)

        approximateCostOfTargetVerificationsRDD.unpersist()

        (cheapVerificationsRDD, skewVerificationsRDD)
    }


    def batchedRedistribution(linkersRDD: RDD[LinkerT]): RDD[IM] ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = partitionVerifications(linkersRDD)

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


    def segmentsVerificationRedistribution(linkersRDD: RDD[LinkerT], theta: TileGranularities): RDD[List[EntityT]] ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = partitionVerifications(linkersRDD)

        val overloadedImRDD = expensiveVerificationsRDD
            .flatMap { entities =>
                val t = entities.head.asInstanceOf[DecomposedEntity]
                val sourceEntities = entities.tail

                // build trie to find which segments need to be verified with the same source geometries
                val trie = IndicesPrefixTrie(t, sourceEntities)

                // extract from trie the verifications
                // create new entity using the segment as its geometry but maintaining its initial envelope
                // if a source entity needs to be verified with multiple segments, we unite them into a single geometry
                val flattenNodes = trie.getFlattenNodes
                flattenNodes.map{ case (targetSegmentIndices, sourceEntities) =>
                    val targetSegments = targetSegmentIndices.map(i => t.segments(i)).asJava
                    val partialTarget = new UnaryUnionOp(targetSegments).union()
                    SpatialEntity(t.originalID, partialTarget, theta, t.approximation) :: sourceEntities
                }
            }
            .repartition(linkersRDD.getNumPartitions)

        simpleVerificationsRDD.union(overloadedImRDD)
    }


    def batchedSegmentedVerificationRedistribution(linkersRDD: RDD[LinkerT], theta:TileGranularities): RDD[List[EntityT]] ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = partitionVerifications(linkersRDD)

        val overloadedImRDD = expensiveVerificationsRDD
            .flatMap { entities =>
                val t = entities.head.asInstanceOf[DecomposedEntity]
                val sourceEntities = entities.tail

                // build trie to find which segments need to be verified with the same source geometries
                val trie = IndicesPrefixTrie(t, sourceEntities)
                trie.balanceTrie()

                // extract from trie the verifications
                // create new entity using the segment as its geometry but maintaining its initial envelope
                // if a source entity needs to be verified with multiple segments, we unite them into a single geometry
                val flattenNodes = trie.getFlattenNodes
                flattenNodes.map{ case (targetSegmentIndices, sourceEntities) =>
                    val targetSegments = targetSegmentIndices.map(i => t.segments(i)).asJava
                    val partialTarget = new UnaryUnionOp(targetSegments).union()
                    SpatialEntity(t.originalID, partialTarget, theta, t.approximation) :: sourceEntities
                }
            }
            .repartition(linkersRDD.getNumPartitions)
        simpleVerificationsRDD.union(overloadedImRDD)
    }

    def executeVerifications(verificationsRDD: RDD[List[EntityT]]): RDD[IM] =
        verificationsRDD.flatMap{ entities =>
            val t = entities.head
            val sourceEntities = entities.tail
            sourceEntities.map(s => s.getIntersectionMatrix(t) )
        }


    /***************************************************************************************************************/

    def timeBatchedRedistribution(linkersRDD: RDD[LinkerT]): Unit ={
        val (simpleVerificationsRDD, expensiveVerificationsRDD) = partitionVerifications(linkersRDD)

        val simpleTimeRDD: RDD[(Int, Double)] = simpleVerificationsRDD
            .mapPartitions{ verificationsI =>
                val pid = TaskContext.getPartitionId()
                val startTime = Calendar.getInstance().getTimeInMillis
                val im = verificationsI.flatMap{ entities =>
                    val t = entities.head
                    val sourceEntities = entities.tail
                    sourceEntities.map(s => s.getIntersectionMatrix(t))
                }
                val endTime = Calendar.getInstance().getTimeInMillis
                val time = (endTime - startTime) / 1000.0
                Iterator((pid, time))
            }

        val expensiveTimeRDD: RDD[(Int, Double)] = expensiveVerificationsRDD
            .mapPartitions{ verificationsI =>
                val verifications = verificationsI.flatMap{ entities =>
                    val t = entities.head
                    val sourceEntities = entities.tail
                    sourceEntities.map(s => (s, t))
                }
                verifications
            }
            .repartition(linkersRDD.getNumPartitions)
            .mapPartitions { verificationsI =>
                val pid = TaskContext.getPartitionId()
                val startTime = Calendar.getInstance().getTimeInMillis
                val times = verificationsI.map { case (s, t) =>
                    val im = s.getIntersectionMatrix(t)
                    val endTime = Calendar.getInstance().getTimeInMillis
                    (endTime - startTime) / 1000.0
                }
                Iterator((pid, times.max))
            }
        log.info()
        log.info("Verification Cost")
        logTime(simpleTimeRDD.union(expensiveTimeRDD))
    }


    def timeSegmentsVerificationRedistribution(linkersRDD: RDD[LinkerT], theta:TileGranularities): Unit ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = partitionVerifications(linkersRDD)

        val simpleTimeRDD: RDD[(Int, Double)] = simpleVerificationsRDD
            .mapPartitions{ verificationsI =>
                val pid = TaskContext.getPartitionId()
                val startTime = Calendar.getInstance().getTimeInMillis
                val im = verificationsI.flatMap{ entities =>
                    val t = entities.head
                    val sourceEntities = entities.tail
                    sourceEntities.map(s => s.getIntersectionMatrix(t))
                }
                val endTime = Calendar.getInstance().getTimeInMillis
                val time = (endTime - startTime) / 1000.0
                Iterator((pid, time))
            }

        val expensiveTimeRDD = expensiveVerificationsRDD
            .flatMap { entities =>
                val t = entities.head.asInstanceOf[DecomposedEntity]
                val sourceEntities = entities.tail

                // build trie to find which segments need to be verified with the same source geometries
                val trie = IndicesPrefixTrie(t, sourceEntities)

                // extract from trie the verifications
                // create new entity using the segment as its geometry but maintaining its initial envelope
                // if a source entity needs to be verified with multiple segments, we unite them into a single geometry
                val flattenNodes = trie.getFlattenNodes
                flattenNodes.map{ case (targetSegmentIndices, sourceEntities) =>
                    val targetSegments = targetSegmentIndices.map(i => t.segments(i)).asJava
                    val partialTarget = new UnaryUnionOp(targetSegments).union()
                    SpatialEntity(t.originalID, partialTarget, theta, t.approximation) :: sourceEntities
                }
            }
            .repartition(linkersRDD.getNumPartitions)
            .mapPartitions { verificationsI =>
                val pid = TaskContext.getPartitionId()
                val startTime = Calendar.getInstance().getTimeInMillis
                val times = verificationsI.map { entities =>
                    val t = entities.head
                    val sourceEntities = entities.tail
                    val im = sourceEntities.map(s => s.getIntersectionMatrix(t))
                    val endTime = Calendar.getInstance().getTimeInMillis
                    (endTime - startTime) / 1000.0
                }
                Iterator((pid, times.max))
            }
        log.info()
        log.info("Verification Cost")
        logTime(simpleTimeRDD.union(expensiveTimeRDD))
    }


    def timeBatchedSegmentedVerificationRedistribution(linkersRDD: RDD[LinkerT], theta:TileGranularities): Unit ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = partitionVerifications(linkersRDD)

        val simpleTimeRDD: RDD[(Int, Double)] = simpleVerificationsRDD
            .mapPartitions{ verificationsI =>
                val pid = TaskContext.getPartitionId()
                val startTime = Calendar.getInstance().getTimeInMillis
                val im = verificationsI.flatMap{ entities =>
                    val t = entities.head
                    val sourceEntities = entities.tail
                    sourceEntities.map(s => s.getIntersectionMatrix(t))
                }
                val endTime = Calendar.getInstance().getTimeInMillis
                val time = (endTime - startTime) / 1000.0
                Iterator((pid, time))
            }

        val expensiveTimeRDD = expensiveVerificationsRDD
            .flatMap { entities =>
                val t = entities.head.asInstanceOf[DecomposedEntity]
                val sourceEntities = entities.tail

                // build trie to find which segments need to be verified with the same source geometries
                val trie = IndicesPrefixTrie(t, sourceEntities)
                trie.balanceTrie()

                // extract from trie the verifications
                // create new entity using the segment as its geometry but maintaining its initial envelope
                // if a source entity needs to be verified with multiple segments, we unite them into a single geometry
                val flattenNodes = trie.getFlattenNodes
                flattenNodes.map{ case (targetSegmentIndices, sourceEntities) =>
                    val targetSegments = targetSegmentIndices.map(i => t.segments(i)).asJava
                    val partialTarget = new UnaryUnionOp(targetSegments).union()
                    SpatialEntity(t.originalID, partialTarget, theta, t.approximation) :: sourceEntities
                }
            }
            .repartition(linkersRDD.getNumPartitions)
            .mapPartitions { verificationsI =>
                val pid = TaskContext.getPartitionId()
                val startTime = Calendar.getInstance().getTimeInMillis
                val times = verificationsI.map { entities =>
                    val t = entities.head
                    val sourceEntities = entities.tail
                    val im = sourceEntities.map(s => s.getIntersectionMatrix(t))
                    val endTime = Calendar.getInstance().getTimeInMillis
                    (endTime - startTime) / 1000.0
                }
                Iterator((pid, times.max))
            }
        log.info()
        log.info("Verification Cost")
        logTime(simpleTimeRDD.union(expensiveTimeRDD))
    }

    def logTime(timesRDD: RDD[(Int, Double)]): Unit ={
        log.info("PID\tTime")
        timesRDD.collect().sortBy(_._1).foreach{case (pid, time) => log.info(pid + "\t" + time)}
    }


}
