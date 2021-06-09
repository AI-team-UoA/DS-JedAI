package interlinkers

import model.entities.Entity
import model.{IM, SpatialIndex, TileGranularities}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation

import scala.math.{max, min}

case class DirtyGIAnt(source:RDD[Entity], partitionBorders: Array[Envelope], tileGranularities: TileGranularities) {


    /**
     * Return true if the reference point is inside the block and inside the partition
     * The reference point is the upper left point of their intersection
     *
     * @param s source entity
     * @param t target entity
     * @param b block
     * @param partition current partition
     * @return true if the reference point is in the block and in partition
     */
    def referencePointFiltering(s: Entity, t: Entity, b:(Int, Int), partition: Envelope): Boolean ={

        val env1 = s.env
        val env2 = t.env
        val epsilon = 1e-8

        val minX1 = env1.getMinX /tileGranularities.x
        val minX2 = env2.getMinX /tileGranularities.x
        val maxY1 = env1.getMaxY /tileGranularities.y
        val maxY2 = env2.getMaxY /tileGranularities.y

        val rfX: Double = max(minX1, minX2)+epsilon
        val rfY: Double = min(maxY1, maxY2)+epsilon

        val blockContainsRF: Boolean =  b._1 <= rfX && b._1+1 >= rfX && b._2 <= rfY && b._2+1 >= rfY
        val partitionContainsRF: Boolean = partition.getMinX <= rfX && partition.getMaxX >= rfX && partition.getMinY <= rfY && partition.getMaxY >= rfY
        blockContainsRF && partitionContainsRF
    }


    /**
     *
     * filter redundant verifications based on spatial criteria
     *
     * @param s source spatial entity
     * @param t source spatial entity
     * @param relation examining relation
     * @param block block the comparison belongs to
     * @param partition the partition the comparisons belong to
     * @return true if comparison is necessary
     */
    def filterVerifications(s: Entity, t: Entity, relation: Relation, block: (Int, Int), partition: Envelope): Boolean =
        s.intersectingMBR(t, relation) && referencePointFiltering(s, t, block, partition)


    def getDE9IM: RDD[IM] = {
        source
            .mapPartitions { p =>
                val partition = partitionBorders(TaskContext.getPartitionId())
                val source: Array[Entity] = p.toArray
                val sourceIndex = SpatialIndex(source, tileGranularities)
                sourceIndex.getIndices.flatMap { b: (Int, Int) =>
                    val candidates = sourceIndex.get(b)
                    for (s <- candidates;
                         t <- candidates
                         if filterVerifications(s, t, Relation.DE9IM, b, partition))
                        yield{
                            s.getIntersectionMatrix(t)
                        }
                }.toIterator
            }
    }
}