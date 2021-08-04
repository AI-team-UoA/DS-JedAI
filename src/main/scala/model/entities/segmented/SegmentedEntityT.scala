package model.entities.segmented

import model.entities.Entity
import org.locationtech.jts.geom.Geometry

trait SegmentedEntityT[T] extends Entity {
    val originalID: String
    val geometry: Geometry
    val segments: Seq[T]

}
