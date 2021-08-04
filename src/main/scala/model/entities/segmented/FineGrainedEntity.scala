package model.entities.segmented

import model.TileGranularities
import model.entities.Entity
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp

case class FineGrainedEntity(originalID: String, geometry: Geometry, segments: Seq[Envelope]) extends SegmentedEntityT[Envelope]{

    def envelopeIntersection(envelopes: Seq[Envelope]): Boolean ={
        for (e1 <- segments; e2 <- envelopes; if EnvelopeOp.checkIntersection(e1, e2, Relation.INTERSECTS))
              return true
        false
    }

    override def intersectingMBR(e: Entity, relation: Relation): Boolean = {
        lazy val segmentsEnvIntersection: Boolean = e match {
            case DecomposedEntity(_, _, targetSegments) =>
                envelopeIntersection(targetSegments.map(_.getEnvelopeInternal))
            case IndexedDecomposedEntity(_, _, targetSegments, _) =>
                envelopeIntersection(targetSegments.map(_.getEnvelopeInternal))
            case FineGrainedEntity(_, _, targetEnvSegments) =>
                envelopeIntersection(targetEnvSegments)
            case _ =>
                envelopeIntersection(Seq(e.env))
        }
        val envIntersection: Boolean = EnvelopeOp.checkIntersection(env, e.env, relation)
        envIntersection && segmentsEnvIntersection
    }
}

object FineGrainedEntity{
    def apply(originalID: String, geom: Geometry, theta: TileGranularities, envelopeRefiner: Geometry => Seq[Envelope]): FineGrainedEntity ={
        FineGrainedEntity(originalID, geom, envelopeRefiner(geom))
    }
}
