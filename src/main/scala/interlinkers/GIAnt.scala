package interlinkers

import model.{Entity, IM}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Utils


case class GIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))], thetaXY: (Double, Double)) extends InterlinkerT {


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
            val partition = partitionsZones(pid)
            val source: Array[Entity] = p._2._1.toArray
            val target: Iterator[Entity] = p._2._2.toIterator
            val sourceIndex = index(source)

            target.flatMap{ e2 =>
                val candidates = getCandidates(e2, source, sourceIndex, partition, relation)
                candidates.filter(e1 => e1.relate(e2, relation))
                    .map(e1 => (e1.originalID, e2.originalID))
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
            val partition = partitionsZones(pid)
            val source: Array[Entity] = p._2._1.toArray
            val target: Iterator[Entity] = p._2._2.toIterator
            val sourceIndex = index(source)

            target.flatMap { e2 =>
                val candidates = getCandidates(e2, source, sourceIndex, partition, Relation.DE9IM)
                candidates.map(e1 => IM(e1, e2)).filter(_.relate)
            }
        }


    def countCandidates: Long =
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source: Array[Entity] = p._2._1.toArray
                val target: Iterable[Entity] = p._2._2
                val sourceIndex = index(source)

                target.flatMap(e2 => getCandidates(e2, source, sourceIndex, partition, Relation.DE9IM))
            }.count()
}

/**
 * auxiliary constructor
 */
object GIAnt{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], partitioner: Partitioner): GIAnt ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        GIAnt(joinedRDD, thetaXY)
    }
}
