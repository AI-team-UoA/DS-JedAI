package model.entities.segmented

import model.approximations.FineGrainedEnvelopes
import model.entities.EntityT
import org.locationtech.jts.geom.{Envelope, Geometry}

trait SegmentedEntityT[T] extends EntityT {
    val originalID: String
    val geometry: Geometry
    val segments: Seq[T]
    val approximation: FineGrainedEnvelopes
}
