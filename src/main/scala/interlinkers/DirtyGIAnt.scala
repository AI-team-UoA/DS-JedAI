package interlinkers

import model.entities.Entity
import model.{IM, MBR, SpatialIndex, TileGranularities}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation

case class DirtyGIAnt(source:RDD[Entity], partitionBorders: Array[MBR], tileGranularities: TileGranularities) {


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
    def filterVerifications(s: Entity, t: Entity, relation: Relation, block: (Int, Int), partition: MBR): Boolean =
        s.intersectingMBR(t, relation) && s.referencePointFiltering(t, block, tileGranularities, partition)


    def getDE9IM: RDD[IM] = {
        source
            .mapPartitions { p =>
                val partition = partitionBorders(TaskContext.getPartitionId())
                val source: Array[Entity] = p.toArray
                val sourceIndex = SpatialIndex(source, tileGranularities)
                sourceIndex.getIndices.flatMap { b: (Int, Int) =>
                    val candidates = sourceIndex.get(b).get
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