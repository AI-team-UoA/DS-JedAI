package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString, Polygon}

/**
 * Initial decomposer trait
 * @tparam T type either Envelope or Geometry
 */
trait DecomposerT[T] {

    val theta: TileGranularities
    val geometryFactory = new GeometryFactory()
    val epsilon: Double = 1e-8
    val xYEpsilon: (Double, Double) =  (epsilon, 0d)

    def decomposePolygon(polygon: Polygon): Seq[T]
    def decomposeLineString(line: LineString): Seq[T]
    def decomposeGeometry(geometry: Geometry) : Seq[T]
    def combineBladeWithInteriorRings(blade: LineString, innerRings: Seq[Geometry], isHorizontal: Boolean): Seq[LineString]
}
