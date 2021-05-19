package utils.decompose

import org.locationtech.jts.geom._
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.decompose.GeometryUtils.flattenCollection

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object RecursiveFragmentation {


    val geometryFactory = new GeometryFactory()
    val epsilon: Double = 1e-8
    val xYEpsilon: (Double, Double) =  (epsilon, 0d)

    def splitBigGeometries(lineThreshold: Double, polygonThreshold: Double)(geometry: Geometry): Seq[Geometry] = {
        geometry match {
            case polygon: Polygon => splitPolygon(polygon, polygonThreshold)
            case line: LineString => splitLineString(line, lineThreshold)
            case gc: GeometryCollection => flattenCollection(gc).flatMap(g => splitBigGeometries(polygonThreshold, polygonThreshold)(g))
            case _ => Seq(geometry)
        }
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

            // epsilon is a small value to add in the segments so to slightly intersect thus not result to dangling lines
            val (xEpsilon, yEpsilon) = if (isHorizontal) xYEpsilon
                                        else xYEpsilon.swap

            // define the line and its points
            // the line will be either vertical or horizontal
            val (start, end) =
                if (isHorizontal) {
                    val start = new Coordinate(env.getMinX-epsilon, y)
                    val end = new Coordinate(env.getMaxX+epsilon, y)
                    (start, end)
                }
                else{
                    val start = new Coordinate(x, env.getMinY-epsilon)
                    val end = new Coordinate(x, env.getMaxY+epsilon)
                    (start, end)
                }
            val line = geometryFactory.createLineString(Array(start, end))

            // define the cross condition based on line direction
            val crossCondition: Envelope => Boolean = if (isHorizontal) env => y >= env.getMinY && y <= env.getMaxY
                                                        else env => x >= env.getMinX && x <= env.getMaxX


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
            val results = (0 until lines.getNumGeometries).map(i => lines.getGeometryN(i).asInstanceOf[LineString])
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
                geometryFactory.createLineString(Array(new Coordinate(env.getMinX-epsilon, midY), new Coordinate(env.getMaxX+epsilon, midY)))
            }
            else{
                val midX = (env.getMaxX + env.getMinX)/2
                geometryFactory.createLineString(Array(new Coordinate(midX, env.getMinY-epsilon), new Coordinate(midX, env.getMaxY+epsilon)))
            }
        }

        // apply
        recursiveSplit(Seq(line), Nil)
    }


}
