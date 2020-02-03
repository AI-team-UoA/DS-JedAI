package DataStructures

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.datasyslab.geospark.formatMapper.WktReader

case class GeoProfile( id: Int, originalID: String = "", attributes: Array[KeyValue] = Array(), geometry: Geometry,
                       maxX:Double, minX:Double, maxY:Double, minY:Double)

object GeoProfile {


    def apply(id: Int, originalID: String, attributes: Array[KeyValue], wkt: String): GeoProfile ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val env = geometry.getEnvelopeInternal

        GeoProfile(id, originalID, attributes, geometry, env.getMaxX, env.getMinX, env.getMaxY, env.getMinY)
    }
}
