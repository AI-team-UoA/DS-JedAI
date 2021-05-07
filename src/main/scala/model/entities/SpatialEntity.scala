package model.entities

import model.MBR
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader

case class SpatialEntity(originalID: String = "", geometry: Geometry, mbr: MBR) extends Entity


/**
 * auxiliary constructors
 */
object SpatialEntity {

    def apply(originalID: String, wkt: String): Entity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val mbb = MBR(geometry)

        SpatialEntity(originalID, geometry, mbb)
    }

    def apply(originalID: String, geom: Geometry): Entity ={
        val geometry: Geometry = geom
        val mbb = MBR(geometry)

        SpatialEntity(originalID, geometry, mbb)
    }

}
