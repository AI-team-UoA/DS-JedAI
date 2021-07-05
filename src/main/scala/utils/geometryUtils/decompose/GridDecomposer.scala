package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom._
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp
import org.locationtech.jts.precision.GeometryPrecisionReducer
import utils.geometryUtils.GeometryUtils.flattenCollection
import collection.JavaConverters._

/**
  * Decompose Geometry based on the grid defined by tile-granularities
  * @param theta tile granularity
 */
case class GridDecomposer(theta: TileGranularities) extends GridDecomposerT[Geometry] {

    val geometryPrecision = 1e+11
    val precisionModel = new PrecisionModel(geometryPrecision)
    val precisionReducer = new GeometryPrecisionReducer(precisionModel)
    precisionReducer.setPointwise(true)
    precisionReducer.setChangePrecisionModel(true)

    /**
     * Decompose a geometry
     * @param geometry geometry
     * @return a list of geometries
     */
    def decomposeGeometry(geometry: Geometry) : Seq[Geometry] = {
        geometry match {
            case polygon: Polygon => decomposePolygon(polygon)
            case line: LineString => decomposeLineString(line)
            case gc: GeometryCollection => flattenCollection(gc).flatMap(g => decomposeGeometry(g))
            case _ => Seq(geometry)
        }
    }

    /**
     * Decompose a polygon into multiple fragments
     * @param polygon polygon
     * @return a list of fragment geometries
     */
    def decomposePolygon(polygon: Polygon): Seq[Geometry] = {

        def split(polygon: Polygon): Seq[Geometry] = {
            val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))
            // find blades based on which to split
            val horizontalBlades = getHorizontalBlades(polygon.getEnvelopeInternal, theta.y)
                .flatMap(b => combineBladeWithInteriorRings(polygon, b, innerRings, isHorizontal = true))
            val verticalBlades = getVerticalBlades(polygon.getEnvelopeInternal, theta.x)
                .flatMap(b => combineBladeWithInteriorRings(polygon, b, innerRings, isHorizontal = false))

            val blades: Seq[Geometry] = verticalBlades ++ horizontalBlades ++ innerRings
            val exteriorRing = polygon.getExteriorRing

            // polygonizer - it will find all possible geometries based on the input geometries
            val polygonizer = new Polygonizer()
            val union = new UnaryUnionOp(blades.asJava).union()
            polygonizer.add(exteriorRing.union(union))
            val fragments = polygonizer.getPolygons.asScala.map(p => p.asInstanceOf[Polygon])

            // filter the polygons that cover holes
            fragments
                .filter(p => polygon.contains(p.getInteriorPoint))
                .map(p => precisionReducer.reduce(p))
                .toSeq
        }

        val env = polygon.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) split(polygon)
        else Seq(polygon)
    }

    /**
     * Decompose a lineString into multiple fragments
     * @param line LineString
     * @return a list of fragment geometries
     */
    def decomposeLineString(line: LineString): Seq[Geometry] = {

        def split(line: LineString): Seq[Geometry] = {
            // find blades based on which to split
            val horizontalBlades = getHorizontalBlades(line.getEnvelopeInternal, theta.y)
            val verticalBlades = getVerticalBlades(line.getEnvelopeInternal, theta.x)
            val blades = geometryFactory.createMultiLineString((verticalBlades ++ horizontalBlades).toArray)

            // find line segments by removing the intersection points
            val lineSegments = line.difference(blades).asInstanceOf[MultiLineString]
            (0 until lineSegments.getNumGeometries).map(i => lineSegments.getGeometryN(i))
                .map(l => precisionReducer.reduce(l))

        }

        val env = line.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) split(line)
        else Seq(line)
    }


}
