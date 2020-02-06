package DataStructures

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader

case class SpatialEntity( id: Int, originalID: String = "", attributes: Array[KeyValue] = Array(), geometry: Geometry,
                       mbb: MBB, crossesMeridian: Boolean)

object SpatialEntity {


    def apply(id: Int, originalID: String, attributes: Array[KeyValue], wkt: String): GeoProfile ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val mbb = MBB(geometry)

        // TODO test if crosses MERIDIAN
        val crossesMeridian =  mbb.crossesMeridian

        SpatialEntity(id, originalID, attributes, geometry, mbb, crossesMeridian)
    }


}
