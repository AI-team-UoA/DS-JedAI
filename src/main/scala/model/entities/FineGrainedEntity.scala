package model.entities

import model.TileGranularities
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp

case class FineGrainedEntity(originalID: String, geometry: Geometry, fineGrainedEnvelopes: Seq[Envelope]) extends Entity{


    def envelopeIntersection(envelopes: Seq[Envelope]): Boolean ={
        val intersections: Iterator[Boolean] = for (e1 <- fineGrainedEnvelopes.iterator; e2 <- envelopes.iterator)
            yield{ EnvelopeOp.checkIntersection(e1, e2, Relation.INTERSECTS)}
        intersections.contains(true)
    }


    override def intersectingMBR(se: Entity, relation: Relation): Boolean =
        se match {
            case fge: FineGrainedEntity =>
                EnvelopeOp.checkIntersection(env, fge.env, Relation.INTERSECTS) &&
                    (EnvelopeOp.checkIntersection(env, fge.env, Relation.CONTAINS) || envelopeIntersection(fge.fineGrainedEnvelopes))

            case e: Entity =>
                EnvelopeOp.checkIntersection(env, e.env, Relation.INTERSECTS) &&
                    (EnvelopeOp.checkIntersection(env, e.env, Relation.CONTAINS) ||
                        EnvelopeOp.checkIntersection(env, e.env, Relation.WITHIN) ||
                        envelopeIntersection(Seq(e.env)))
        }

}

object FineGrainedEntity{
    def apply(originalID: String, geom: Geometry, theta: TileGranularities): FineGrainedEntity ={
        FineGrainedEntity(originalID, geom, EnvelopeOp.getFineGrainedEnvelope(geom, theta))
    }
}
