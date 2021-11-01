package linkers

import cats.implicits._
import model.entities.EntityT
import model.{IM, TileGranularities}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.readers.GridPartitioner

import java.util.Calendar
import scala.math.Numeric.IntIsIntegral

/**
 * Apply distributed Interlinking by initializing
 * a different linker in each partition
 */
object DistributedInterlinking {
    val BATCH_SIZE = 4096
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)
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
    def initializeLinkers(source: RDD[(Int, EntityT)], target: RDD[(Int, EntityT)], partitionBorders: Array[Envelope],
                          theta: TileGranularities, gridPartitioner: GridPartitioner): RDD[LinkerT] = {
        val joinedRDD: RDD[(Int, (Iterable[EntityT], Iterable[EntityT]))] = source.cogroup(target, gridPartitioner.hashPartitioner)
        joinedRDD.map { case (pid: Int, (sourceP: Iterable[EntityT], targetP: Iterable[EntityT])) =>
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


    def executionStats(source: RDD[(Int, EntityT)], target: RDD[(Int, EntityT)], partitionBorders: Array[Envelope],
                       theta: TileGranularities, gridPartitioner: GridPartitioner): Unit ={

        val joinedRDD: RDD[(Int, (Iterable[EntityT], Iterable[EntityT]))] = source.cogroup(target, gridPartitioner.hashPartitioner)
        val linkersRDD: RDD[(Iterator[GIAnt], Long)] = joinedRDD.mapPartitions { iter =>
            val startTime = Calendar.getInstance().getTimeInMillis
            val linkers = iter.map { case (pid: Int, (sourceP: Iterable[EntityT], targetP: Iterable[EntityT])) =>
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
            val totalVerifications = verifications.map(vl => vl.tail.length).sum
            val (maxPoints, totalVerificationsWithMax) = if (totalVerifications > 0) verifications.map { entities => (entities.head.geometry.getNumPoints, entities.tail.length)}.max else (0,0)
            (TaskContext.getPartitionId(), totalVerifications, maxPoints, totalVerificationsWithMax)
        }.sortBy(_._1).collect()

        basicStats.zip(timePerPartition).foreach{ case( (pid, verifications, points, totalMax), (_, time)) =>
            log.info(pid+"\t"+verifications+"\t"+points+"\t"+totalMax+"\t"+time)}
    }

    def timeGiant(source: RDD[(Int, EntityT)], target: RDD[(Int, EntityT)], partitionBorders: Array[Envelope],
                  theta: TileGranularities, gridPartitioner: GridPartitioner): Unit ={

        val joinedRDD: RDD[(Int, (Iterable[EntityT], Iterable[EntityT]))] = source.cogroup(target, gridPartitioner.hashPartitioner)
        val linkersRDD: RDD[(Iterator[GIAnt], Long)] = joinedRDD.mapPartitions { iter =>
            val startTime = Calendar.getInstance().getTimeInMillis
            val linkers = iter.map { case (pid: Int, (sourceP: Iterable[EntityT], targetP: Iterable[EntityT])) =>
                val partition = partitionBorders(pid)
                GIAnt(sourceP.toArray, targetP, theta, partition)
            }
            Iterator((linkers, startTime))
        }

        val timesRDD = linkersRDD.mapPartitions { linkerI =>
            val pid = TaskContext.getPartitionId()
            val time = linkerI.map { case (linkers, startTime) =>
                linkers.foreach(l => l.getDE9IM)
                val endTime = Calendar.getInstance().getTimeInMillis
                (endTime - startTime) / 1000.0
            }.max
            Iterator((pid, time))
        }
        logTime(timesRDD)
    }

    def logTime(timesRDD: RDD[(Int, Double)]): Unit ={
        log.info("PID\tTime")
        timesRDD.collect().sortBy(_._1).foreach{case (pid, time) => log.info(pid + "\t" + time)}
    }
}
