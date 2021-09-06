package model.entities

import model.approximations.{GeometryApproximationT, MBR}
import org.locationtech.jts.geom.Geometry

case class SpatialEntity(originalID: String, geometry: Geometry, approximation: GeometryApproximationT) extends EntityT

object SpatialEntity{

    def apply(originalID: String, geometry: Geometry): SpatialEntity = {
        SpatialEntity(originalID, geometry, MBR(geometry.getEnvelopeInternal))
    }

    def apply(originalID: String, geometry: Geometry, approximationTransformer: Geometry => GeometryApproximationT): SpatialEntity = {
        SpatialEntity(originalID, geometry, approximationTransformer(geometry))
    }
}