package model.entities

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader

case class SpatialEntity(originalID: String, geometry: Geometry) extends Entity


/**
 * auxiliary constructors
 */
object SpatialEntity {

    def apply(originalID: String, wkt: String): SpatialEntity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)

        SpatialEntity(originalID, geometry)
    }
}
