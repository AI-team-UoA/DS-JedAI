package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom._
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp
import org.locationtech.jts.precision.GeometryPrecisionReducer
import utils.geometryUtils.GeometryUtils.flattenCollection
import collection.JavaConverters._


case class GridDecomposer(theta: TileGranularities) extends GridDecomposerT[Geometry] {

    val geometryPrecision = 1e+11
    val precisionModel = new PrecisionModel(geometryPrecision)
    val precisionReducer = new GeometryPrecisionReducer(precisionModel)
    precisionReducer.setPointwise(true)
    precisionReducer.setChangePrecisionModel(true)


    def decomposeGeometry(geometry: Geometry) : Seq[Geometry] = {
        geometry match {
            case polygon: Polygon => decomposePolygon(polygon)
            case line: LineString => decomposeLineString(line)
            case gc: GeometryCollection => flattenCollection(gc).flatMap(g => decomposeGeometry(g))
            case _ => Seq(geometry)
        }
    }


    def decomposePolygon(polygon: Polygon): Seq[Geometry] = {

        def split(polygon: Polygon): Seq[Geometry] = {
            val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))

            val horizontalBlades = getHorizontalBlades(polygon.getEnvelopeInternal, theta.y)
                .flatMap(b => combineBladeWithInteriorRings(polygon, b, innerRings, isHorizontal = true))
            val verticalBlades = getVerticalBlades(polygon.getEnvelopeInternal, theta.x)
                .flatMap(b => combineBladeWithInteriorRings(polygon, b, innerRings, isHorizontal = false))

            val blades: Seq[Geometry] = verticalBlades ++ horizontalBlades ++ innerRings
            val exteriorRing = polygon.getExteriorRing

            val polygonizer = new Polygonizer()
            val union = new UnaryUnionOp(blades.asJava).union()

            polygonizer.add(exteriorRing.union(union))

            val newPolygons = polygonizer.getPolygons.asScala.map(p => p.asInstanceOf[Polygon])
                .filter(p => polygon.contains(p.getInteriorPoint))
                .map(p => precisionReducer.reduce(p))
                .toSeq
            newPolygons
        }

        val env = polygon.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) split(polygon)
        else Seq(polygon)
    }


    def decomposeLineString(line: LineString): Seq[Geometry] = {

        def split(line: LineString): Seq[Geometry] = {
            val horizontalBlades = getHorizontalBlades(line.getEnvelopeInternal, theta.y)
            val verticalBlades = getVerticalBlades(line.getEnvelopeInternal, theta.x)
            val blades = geometryFactory.createMultiLineString((verticalBlades ++ horizontalBlades).toArray)
            val lineSegments = line.difference(blades).asInstanceOf[MultiLineString]
            (0 until lineSegments.getNumGeometries).map(i => lineSegments.getGeometryN(i))
                .map(l => precisionReducer.reduce(l))

        }

        val env = line.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) split(line)
        else Seq(line)
    }


}
