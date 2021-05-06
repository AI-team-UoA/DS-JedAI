package utils

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object GeometryUtils {

    val csf: CoordinateArraySequenceFactory = CoordinateArraySequenceFactory.instance()
    val geometryFactory = new GeometryFactory()

    val epsilon: Double = 1e-8


    def flattenCollection(collection: Geometry): Seq[Geometry] =
        for (i <- 0 until collection.getNumGeometries) yield {
            val g = collection.getGeometryN(i)
            g.setUserData(collection.getUserData)
            g
        }


    def flattenCollections(srdd: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
        srdd.rawSpatialRDD = srdd.rawSpatialRDD.rdd.flatMap(g => if (g.getNumGeometries > 1) flattenCollection(g) else Seq(g))
        srdd
    }


    def splitBigGeometries(geometry: Geometry, areaThreshold: Double = 1e-3): List[Geometry] = {
        geometry match {
            case polygon: Polygon if polygon.getEnvelopeInternal.getArea > areaThreshold=>
                val polygons = splitPolygon(polygon, areaThreshold)
                polygons

            case _ => List(geometry)
        }
    }

    def splitPolygon(polygon: Polygon,  widthT: Double = 0.02, heightT: Double = 0.02): List[Polygon] = {

        /**
         * Recursively, split the polygons into sub-polygons. The procedure is repeated
         * until the width and height of the produced polygons do not exceed predefined thresholds.
         *
         * @param polygons      a list of Polygons
         * @param widthT        width threshold
         * @param heightT       height threshold
         * @param accumulator   the list of sub-polygons produced in the previous recursion
         * @return A list of sub-polygons
         */
        @tailrec
        def recursivePolygonSplit(polygons: List[Polygon], widthT: Double, heightT: Double, accumulator: List[Polygon] = Nil): List[Polygon] = {
            val (bigPolygons, smallPolygons) = polygons.partition(p => p.getEnvelopeInternal.getWidth > widthT || p.getEnvelopeInternal.getHeight > heightT)
            val (widePolygons, nonWide) = bigPolygons.partition(p => p.getEnvelopeInternal.getWidth > widthT)
            val (tallPolygons, nonTall) = bigPolygons.partition(p => p.getEnvelopeInternal.getHeight > heightT)
            if (widePolygons.nonEmpty) {
                val newPolygons = widePolygons.flatMap(p => split(p, isHorizontal = false))
                recursivePolygonSplit(newPolygons ++ nonWide, widthT, heightT, smallPolygons ++ accumulator )
            }
            else if (tallPolygons.nonEmpty) {
                    val newPolygons = tallPolygons.flatMap(p => split(p, isHorizontal = true))
                    recursivePolygonSplit(newPolygons ++ nonTall, widthT, heightT, smallPolygons ++ accumulator)
                }
            else
                smallPolygons ::: accumulator
        }

        /**
         * Split a polygon using either an horizontal or a vertical line
         *
         * @param polygon input polygon
         * @param isHorizontal split polygon horizontally or vertically
         * @return a list of sub-polygons
         */
        def split(polygon: Polygon, isHorizontal: Boolean): List[Polygon] ={
            val exteriorRing = polygon.getExteriorRing
            val interiorRings = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i)).toList
            val blade = getBlade(polygon, isHorizontal)

            val polygonizer = new Polygonizer()
            val innerGeom: List[Geometry] = blade ++ interiorRings
            val union = new UnaryUnionOp(innerGeom.asJava).union()

            polygonizer.add(exteriorRing.union(union))

            val newPolygons = polygonizer.getPolygons.asScala.map(p => p.asInstanceOf[Polygon])
            val f1 = newPolygons.filter(p => polygon.contains(p.getInteriorPoint))
            f1.toList
        }


        /**
         * Get a horizontal or vertical blade that passes from the centroid of the Polygon
         * In case the polygon contains inner holes, then adjust the lines so to not overlap the holes
         *
         *                _________|__________
         *               /  _      |          /
         *          ----/--/_\-----|---------/-------
         *             |           |        /
         *              \_________ |_______/
         *                         |
         *
         * @param polygon input polygon
         * @param isHorizontal the requested blade is horizontal otherwise it will be vertical
         * @return a blade that passes through the centroing of polygon
         */
        def getBlade(polygon: Polygon, isHorizontal: Boolean): List[LineString] = {
            val centroid = polygon.getCentroid
            val env = polygon.getEnvelopeInternal
            val x = centroid.getX
            val y = centroid.getY

            // define the line and its points
            // the line will be either vertical or horizontal
            val (line, start, end) = if (isHorizontal) {
                val start = new Coordinate(env.getMinX, y)
                val end = new Coordinate(env.getMaxX, y)
                val line = geometryFactory.createLineString(Array(start, end))
                (line, start, end)
            }
            else{
                val start = new Coordinate(x, env.getMinY)
                val end = new Coordinate(x, env.getMaxY)
                val line = geometryFactory.createLineString(Array(start, end))
                (line, start, end)

            }

            // define the cross condition based on line direction
            val crossCondition: Envelope => Boolean =
                if (isHorizontal) env => y >= env.getMinY && y <= env.getMaxY
                else env => x >= env.getMinX && x <= env.getMaxX

            // epsilon is a small value to add in the segments so to slightly intersect thus not result to dangling lines
            val (xEpsilon, yEpsilon) = if (isHorizontal) (epsilon, 0d) else (0d, epsilon)

            // ordering of the coordinates, to define max and min
            implicit val ordering: Ordering[Coordinate] = Ordering.by[Coordinate, Double](c => if (isHorizontal) c.x else c.y)

            // sort inner rings envelope by y
            // find the ones that intersect with the line
            // for each intersecting inner ring find the intersection coordinates,
            //   - sort them and get the first and the last,
            //   - create line segments that do not overlap the inner ring
            var checkpoint = start
            val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))
            val segments = innerRings
                .map(ir => (ir, ir.getEnvelopeInternal))
                .filter{ case (_, env) => crossCondition(env)}
                .sortBy{ case (_, env) => if (isHorizontal) env.getMinX else env.getMinY }
                .map{ case (ir, _) =>

                    val intersectingCollection = ir.intersection(line)
                    val ip: Seq[Coordinate] = (0 until intersectingCollection.getNumGeometries)
                        .map(i => intersectingCollection.getGeometryN(i))
                        .flatMap(g => g.getCoordinates)

                    val stopPoint = ip.min(ordering)
                    stopPoint.setX(stopPoint.x+xEpsilon)
                    stopPoint.setY(stopPoint.y+yEpsilon)

                    val newStart = ip.max(ordering)
                    newStart.setX(stopPoint.x+xEpsilon)
                    newStart.setY(stopPoint.y+yEpsilon)

                    val segment = geometryFactory.createLineString(Array(checkpoint, stopPoint))

                    checkpoint = newStart
                    segment
                }.toList

            // create the last line segment
            val segment = geometryFactory.createLineString(Array(checkpoint, end))
            segment ::  segments
        }


        recursivePolygonSplit(List(polygon), widthT, heightT)
    }
}
