package model.entities

import model.TileGranularities
import model.approximations.{GeometryApproximationT, MBR}
import org.locationtech.jts.geom.Geometry

case class SpatialEntity(originalID: String, geometry: Geometry, theta: TileGranularities, approximation: GeometryApproximationT) extends EntityT

object SpatialEntity{

    def apply(originalID: String, geometry: Geometry, theta: TileGranularities): SpatialEntity = {
        SpatialEntity(originalID, geometry, theta, MBR(geometry.getEnvelopeInternal))
    }

    def apply(originalID: String, geometry: Geometry, theta: TileGranularities, approximationTransformer: Geometry => GeometryApproximationT): SpatialEntity = {
        SpatialEntity(originalID, geometry, theta, approximationTransformer(geometry))
    }
}