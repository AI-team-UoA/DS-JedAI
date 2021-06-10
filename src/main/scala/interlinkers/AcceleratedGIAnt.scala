package interlinkers

import ai.di.uoa.gr.EnhancedGeometry
import model.entities.Entity
import model.{IM, SpatialIndex, TileGranularities}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.algorithm.{LineIntersector, RobustLineIntersector}
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.operation.relate.RelateComputer
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation

case class AcceleratedGIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                            tileGranularities: TileGranularities, partitionBorders: Array[Envelope])
    extends InterlinkerT {



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
            val target: Iterable[Entity] = p._2._2
            val sourceIndex = SpatialIndex(source, tileGranularities)

            val rc = new RelateComputer()
            val lineIntersector: LineIntersector = new RobustLineIntersector()
            val enhancedSource: Array[EnhancedGeometry] = source.map(e => new EnhancedGeometry(e.geometry, 0, lineIntersector))


            target.flatMap { t =>
                val enhancedT = new EnhancedGeometry(t.geometry, 1, lineIntersector)
                getAllCandidatesWithIndex(t, sourceIndex, partition, Relation.DE9IM).map{ case (i, s) =>
                    val enhancedS = enhancedSource(i)
                    val im = enhancedS.relate(enhancedT, rc, lineIntersector)
                    IM(s, t, im)
                }
            }
        }
}

/**
 * auxiliary constructor
 */
object AcceleratedGIAnt{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], tileGranularities: TileGranularities,
              partitionBorders: Array[Envelope], partitioner: Partitioner): AcceleratedGIAnt ={
        val joinedRDD = source.cogroup(target, partitioner)


        AcceleratedGIAnt(joinedRDD, tileGranularities, partitionBorders)
    }
}
