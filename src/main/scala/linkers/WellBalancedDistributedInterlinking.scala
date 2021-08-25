package linkers

import linkers.DistributedInterlinking.log
import model.entities.segmented.DecomposedEntity
import model.entities.{Entity, SpatialEntity}
import model.{IM, IndicesPrefixTrie, TileGranularities}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.readers.GridPartitioner

import java.util.Calendar
import scala.collection.JavaConverters._

object WellBalancedDistributedInterlinking {

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


    def segmentsVerificationRedistribution(linkersRDD: RDD[LinkerT]): RDD[List[Entity]] ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = getOutliersRDD(linkersRDD, threshold = 3d)

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
                    SpatialEntity(t.originalID, partialTarget, t.env) :: sourceEntities
                }
            }
            .repartition(linkersRDD.getNumPartitions)

        simpleVerificationsRDD.union(overloadedImRDD)
    }


    def batchedSegmentedVerificationRedistribution(linkersRDD: RDD[LinkerT]): RDD[List[Entity]] ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = getOutliersRDD(linkersRDD, threshold = 3d)

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
                    SpatialEntity(t.originalID, partialTarget, t.env) :: sourceEntities
                }
            }
            .repartition(linkersRDD.getNumPartitions)
        simpleVerificationsRDD.union(overloadedImRDD)
    }

    def executeVerifications(verificationsRDD: RDD[List[Entity]]): RDD[IM] =
        verificationsRDD.flatMap{ entities =>
            val t = entities.head
            val sourceEntities = entities.tail
            sourceEntities.map(s => s.getIntersectionMatrix(t) )
        }


    /***************************************************************************************************************/

    def timeBatchedRedistribution(linkersRDD: RDD[LinkerT]): Unit ={
        val (simpleVerificationsRDD, expensiveVerificationsRDD) = getOutliersRDD(linkersRDD, threshold = 3d)

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


    def timeSegmentsVerificationRedistribution(linkersRDD: RDD[LinkerT]): Unit ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = getOutliersRDD(linkersRDD, threshold = 3d)

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
                    SpatialEntity(t.originalID, partialTarget, t.env) :: sourceEntities
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


    def timeBatchedSegmentedVerificationRedistribution(linkersRDD: RDD[LinkerT]): Unit ={

        val (simpleVerificationsRDD, expensiveVerificationsRDD) = getOutliersRDD(linkersRDD, threshold = 3d)

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
                    SpatialEntity(t.originalID, partialTarget, t.env) :: sourceEntities
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
