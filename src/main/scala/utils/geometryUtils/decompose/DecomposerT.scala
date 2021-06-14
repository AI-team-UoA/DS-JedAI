package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString, Polygon}

trait DecomposerT {

    val theta: TileGranularities
    val geometryFactory = new GeometryFactory()
    val epsilon: Double = 1e-8
    val xYEpsilon: (Double, Double) =  (epsilon, 0d)

    def splitPolygon(polygon: Polygon): Seq[Geometry]
    def splitLineString(line: LineString): Seq[Geometry]
    def splitBigGeometries(geometry: Geometry) : Seq[Geometry]
    def combineBladeWithInteriorRings(polygon: Polygon, blade: LineString, innerRings: Seq[Geometry], isHorizontal: Boolean): Seq[LineString]
}
