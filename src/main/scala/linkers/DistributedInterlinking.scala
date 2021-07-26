package linkers

import model.entities.Entity
import model.{IM, TileGranularities}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import cats.implicits._
import utils.readers.GridPartitioner


/**
 * Apply distributed Interlinking by initializing
 * a different linker in each partition
 */
object DistributedInterlinking {

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

    /**
     * Compute the number of verifications provided an RDD of linkers
     * @param linkersRDD an RDD of linkers
     * @return the number of Verifications
     */
    def countVerifications(linkersRDD: RDD[LinkerT]): Double =
        linkersRDD.map(linker => linker.countVerification).sum

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
    def countAllRelations(linkersRDD: RDD[LinkerT]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
        accumulateIM(computeIM(linkersRDD))

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
}
