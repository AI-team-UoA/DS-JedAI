package linkers

import model.{IM, SpatialIndex, TileGranularities}
import model.entities.Entity
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation

case class GIAnt(source: Array[Entity], target: Iterable[Entity], tileGranularities: TileGranularities,
                 partitionBorder: Envelope) extends LinkerT {

    /**
     * First index the Source and then use the index to find the comparisons with target's entities.
     * Filter the redundant comparisons using testMBR and RF
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    override def relate(relation: Relation): Iterator[(String, String)] = {
        target.flatMap{ t =>
            getAllCandidates(t, sourceIndex, partitionBorder, relation)
                .filter(s => s.relate(t, relation))
                .map(s => (s.originalID, t.originalID))
        }
    }.toIterator


    /**
     * compute the Intersection Matrix of the input datasets
     * @return an RDD of intersection matrix
     */
    override def getDE9IM: Iterator[IM] = {
            target.flatMap { t =>
                getAllCandidates(t, sourceIndex, partitionBorder, Relation.DE9IM)
                    .map(s => s.getIntersectionMatrix(t))
            }
        }.toIterator
}
