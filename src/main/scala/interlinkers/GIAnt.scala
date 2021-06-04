package interlinkers

import model.entities.Entity
import model.{IM, SpatialIndex, TileGranularities}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.Constants.Relation
import utils.Constants.Relation.Relation


case class GIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))], tileGranularities: TileGranularities,
                 partitionBorders: Array[Envelope]) extends InterlinkerT {


    /**
     * First index the Source and then use the index to find the comparisons with target's entities.
     * Filter the redundant comparisons using testMBR and RF
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    override def relate(relation: Relation): RDD[(String, String)] = joinedRDD
        .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty )
        .flatMap { p =>
            val pid = p._1
            val partition = partitionBorders(pid)
            val source: Array[Entity] = p._2._1.toArray
            val target: Iterator[Entity] = p._2._2.toIterator
            val sourceIndex = SpatialIndex(source, tileGranularities)

            target.flatMap{ t =>
                getAllCandidates(t, sourceIndex, partition, relation)
                    .filter(s => s.relate(t, relation))
                    .map(s => (s.originalID, t.originalID))
            }
        }


    /**
     * compute the Intersection Matrix of the input datasets
     * @return an RDD of intersection matrix
     */
    override def getDE9IM: RDD[IM] = joinedRDD
        .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
        .flatMap { p =>
            val pid = p._1
            val partition = partitionBorders(pid)
            val source: Array[Entity] = p._2._1.toArray
            val target: Iterator[Entity] = p._2._2.toIterator
            val sourceIndex = SpatialIndex(source, tileGranularities)

            target.flatMap { t =>
                getAllCandidates(t, sourceIndex, partition, Relation.DE9IM)
                    .map(s => s.getIntersectionMatrix(t))
            }
        }
}

/**
 * auxiliary constructor
 */
object GIAnt{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], tileGranularities: TileGranularities,
              partitionBorders: Array[Envelope], partitioner: Partitioner): GIAnt ={
        val joinedRDD = source.cogroup(target, partitioner)
        GIAnt(joinedRDD, tileGranularities, partitionBorders)
    }
}
