package model.entities

import org.locationtech.jts.geom.{Envelope, Geometry}

case class SpatialEntity(originalID: String, geometry: Geometry, env: Envelope) extends Entity

object SpatialEntity{

    def apply(originalID: String, geometry: Geometry): SpatialEntity = SpatialEntity(originalID, geometry, geometry.getEnvelopeInternal)
}