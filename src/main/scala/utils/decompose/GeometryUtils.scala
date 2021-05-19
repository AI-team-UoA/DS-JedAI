package utils.decompose

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.locationtech.jts.geom._

object GeometryUtils {

    def flattenCollection(collection: Geometry): Seq[Geometry] =
        for (i <- 0 until collection.getNumGeometries) yield {
            val g = collection.getGeometryN(i)
            g.setUserData(collection.getUserData)
            g
        }


    def flattenSRDDCollections(srdd: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
        srdd.rawSpatialRDD = srdd.rawSpatialRDD.rdd.flatMap(g => if (g.getNumGeometries > 1) flattenCollection(g) else Seq(g))
        srdd
    }




}
