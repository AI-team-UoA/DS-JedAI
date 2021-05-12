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


    def flattenSRDDCollections(srdd: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
        srdd.rawSpatialRDD = srdd.rawSpatialRDD.rdd.flatMap(g => if (g.getNumGeometries > 1) flattenCollection(g) else Seq(g))
        srdd
    }


    def splitBigGeometries(lineThreshold: Double, polygonThreshold: Double)(geometry: Geometry): Seq[Geometry] = {
        val res = geometry match {
            case polygon: Polygon => splitPolygon(polygon, polygonThreshold)
            case line: LineString => splitLineString(line, lineThreshold)
            case gc: GeometryCollection => flattenCollection(gc).flatMap(g => splitBigGeometries(polygonThreshold, polygonThreshold)(g))
            case _ => Seq(geometry)
        }
        res
    }


    /**
     * Split polygons into smaller polygons. Splitting is determined by the threshold.
     * The width and the height of the produced polygons will not exceed the threshold
     *
     * @param polygon       input polygon
     * @param threshold     input threshold
     * @return              a seq of smaller polygons
     */
    def splitPolygon(polygon: Polygon, threshold: Double): Seq[Polygon] = {

        /**
         * Recursively, split the polygons into sub-polygons. The procedure is repeated
         * until the width and height of the produced polygons do not exceed predefined thresholds.
         *
         * @param polygons      a list of Polygons
         * @param accumulator   the list of sub-polygons produced in the previous recursion
         * @return A list of sub-polygons
         */
        @tailrec
        def recursiveSplit(polygons: Seq[Polygon], accumulator: Seq[Polygon] = Nil): Seq[Polygon] = {
            val (bigPolygons, smallPolygons) = polygons.partition(p => p.getEnvelopeInternal.getWidth > threshold || p.getEnvelopeInternal.getHeight > threshold)
            val (widePolygons, nonWide) = bigPolygons.partition(p => p.getEnvelopeInternal.getWidth > threshold)
            val (tallPolygons, nonTall) = bigPolygons.partition(p => p.getEnvelopeInternal.getHeight > threshold)
            if (widePolygons.nonEmpty) {
                val newPolygons = widePolygons.flatMap(p => split(p,  getBlade(p, isHorizontal = false)))
                recursiveSplit(newPolygons ++ nonWide, smallPolygons ++ accumulator )
            }
            else if (tallPolygons.nonEmpty) {
                val newPolygons = tallPolygons.flatMap(p => split(p, getBlade(p, isHorizontal = true)))
                recursiveSplit(newPolygons ++ nonTall, smallPolygons ++ accumulator)
            }
            else
                smallPolygons ++ accumulator
        }

        /**
         * Split a polygon using the input blade
         *
         * @param polygon input polygon
         * @param blade line segments divide the polygon
         * @return a Seq of sub-polygons
         */
        def split(polygon: Polygon, blade: Seq[LineString]): Seq[Polygon] ={
            val exteriorRing = polygon.getExteriorRing
            val interiorRings = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))

            val polygonizer = new Polygonizer()
            val innerGeom: Seq[Geometry] = blade ++ interiorRings
            val union = new UnaryUnionOp(innerGeom.asJava).union()

            polygonizer.add(exteriorRing.union(union))

            val newPolygons = polygonizer.getPolygons.asScala.map(p => p.asInstanceOf[Polygon])
            newPolygons.filter(p => polygon.contains(p.getInteriorPoint)).toSeq
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
        def getBlade(polygon: Polygon, isHorizontal: Boolean): Seq[LineString] = {
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

        // apply
        recursiveSplit(List(polygon))
    }




    /**
     * Split linestring into smaller linestrings. Splitting is determined by the threshold.
     * The width and the height of the produced linestrings will not exceed the threshold
     *
     * @param line           input linestring
     * @param threshold     input threshold
     * @return              a seq of smaller linestring
     */
    def splitLineString(line: LineString, threshold: Double = 2e-2): Seq[LineString] = {

        /**
         * Recursively, split the linestring into sub-lines. The procedure is repeated
         * until the width and height of the produced lines do not exceed predefined thresholds.
         *
         * @param lines         a seq of lineStrings
         * @param accumulator   the list of sub-lines produced in the previous recursion
         * @return              A list of sub-polygons
         */
        @tailrec
        def recursiveSplit(lines: Seq[LineString], accumulator: Seq[LineString] = Nil): Seq[LineString] ={
            val (bigLines, smallLines) = lines.partition(l => l.getEnvelopeInternal.getWidth > threshold || l.getEnvelopeInternal.getHeight > threshold)
            val (wideLines, nonWide) = bigLines.partition(p => p.getEnvelopeInternal.getWidth > threshold)
            val (tallLines, nonTall) = bigLines.partition(p => p.getEnvelopeInternal.getHeight > threshold)
            if (wideLines.nonEmpty) {
                val newLines = wideLines.flatMap(l => split(l, getBlade(l, isHorizontal = false) ))
                recursiveSplit(newLines ++ nonWide, smallLines ++ accumulator )
            }
            else if (tallLines.nonEmpty) {
                val newLines = tallLines.flatMap(l => split(l, getBlade(l, isHorizontal = true)))
                recursiveSplit(newLines ++ nonTall, smallLines ++ accumulator)
            }
            else
                smallLines ++ accumulator
        }

        /**
         * Split a line using the input blade
         *
         * @param line  input line
         * @param blade line segments divide the line
         * @return      a Seq of sub-line
         */
        def split(line: LineString, blade: LineString): Seq[LineString] = {
            val lines = line.difference(blade).asInstanceOf[MultiLineString]
            val results = (0 until  lines.getNumGeometries).map(i => lines.getGeometryN(i).asInstanceOf[LineString])
            results
        }


        /**
         * Get a blade that crosses the line in the middle vertical or horizontal point
         *
         * @param line          input line
         * @param isHorizontal  the requested blade is horizontal otherwise it will be vertical
         * @return              a linestring
         */
        def getBlade(line: LineString, isHorizontal: Boolean): LineString = {
            val env = line.getEnvelopeInternal
            if (isHorizontal){
                val midY = (env.getMaxY + env.getMinY)/2
                geometryFactory.createLineString(Array(new Coordinate(env.getMinX, midY), new Coordinate(env.getMaxX, midY)))
            }
            else{
                val midX = (env.getMaxX + env.getMinX)/2
                geometryFactory.createLineString(Array(new Coordinate(midX, env.getMinY), new Coordinate(midX, env.getMaxY)))
            }
        }

        // apply
        recursiveSplit(Seq(line), Nil)
    }


}
