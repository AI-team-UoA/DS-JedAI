package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom._
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.geometryUtils.GeometryUtils.flattenCollection

import scala.annotation.tailrec
import collection.JavaConverters._

case class RecursiveDecomposer(theta: TileGranularities) extends DecomposerT[Geometry] {

    def decomposeGeometry(geometry: Geometry)(implicit oneDimension: Boolean=false): Seq[Geometry] = {
        geometry match {
            case polygon: Polygon => if(oneDimension) decomposePolygon1D(polygon) else decomposePolygon(polygon)
            case line: LineString => decomposeLineString(line)
            case gc: GeometryCollection => flattenCollection(gc).flatMap(g => decomposeGeometry(g))
            case _ => Seq(geometry)
        }
    }


    def getVerticalBlade(geom: Geometry): LineString = {
        val env = geom.getEnvelopeInternal
        val x = geom match {
            case p: Polygon => p.getCentroid.getX
            case _ => (env.getMaxX + env.getMinX)/2
        }
        val start = new Coordinate(x, env.getMinY-epsilon)
        val end = new Coordinate(x, env.getMaxY+epsilon)
        geometryFactory.createLineString(Array(start, end))
    }


    def getHorizontalBlade(geom: Geometry): LineString = {
        val env = geom.getEnvelopeInternal
        val y = geom match {
            case p: Polygon => p.getCentroid.getY
            case _ => (env.getMaxY + env.getMinY)/2
        }
        val start = new Coordinate(env.getMinX-epsilon, y)
        val end = new Coordinate(env.getMaxX+epsilon, y)
        geometryFactory.createLineString(Array(start, end))
    }


    /**
     * Adjust the horizontal or vertical blade that passes through polygon's inner holes.
     * In case the polygon contains inner holes, then adjust the lines so to not overlap the holes
     *
     *                _________|__________
     *               /  _ _    |          /
     *          ----/--/_ _|---|---------/-------
     *             |           |        /
     *              \_________ |_______/
     *                         |
     *
     * @param polygon input polygon
     * @param blade the blade to combine with the inner holes
     * @param innerRings the inner holes as a sequence of inner lineRings
     * @param isHorizontal the requested blade is horizontal otherwise it will be vertical
     * @return
     */
    def combineBladeWithInteriorRings(polygon: Polygon, blade: LineString, innerRings: Seq[Geometry], isHorizontal: Boolean): Seq[LineString] = {

        // ordering of the coordinates, to define max and min
        implicit val ordering: Ordering[Coordinate] = Ordering.by[Coordinate, Double](c => if (isHorizontal) c.x else c.y)

        // epsilon is a small value to add in the segments so to slightly intersect thus not result to dangling lines
        val (xEpsilon, yEpsilon) = if (isHorizontal) xYEpsilon else xYEpsilon.swap

        // the blade is a linestring formed by two points
        val start = blade.getCoordinateN(0)
        val end = blade.getCoordinateN(1)

        // define the cross condition based on line's orientation
        val crossCondition: Envelope => Boolean = if (isHorizontal) env => start.y >= env.getMinY && start.y <= env.getMaxY
                                                    else env => start.x >= env.getMinX && start.x <= env.getMaxX

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

                val intersectingCollection = ir.intersection(blade)
                val ip: Seq[Coordinate] = (0 until intersectingCollection.getNumGeometries)
                    .map(i => intersectingCollection.getGeometryN(i))
                    .flatMap(g => g.getCoordinates)

                val stopPoint = ip.min(ordering)
                stopPoint.setX(stopPoint.x+xEpsilon)
                stopPoint.setY(stopPoint.y+yEpsilon)

                val newStart = ip.max(ordering)
                newStart.setX(newStart.x-xEpsilon)
                newStart.setY(newStart.y-yEpsilon)

                val segment = geometryFactory.createLineString(Array(checkpoint, stopPoint))

                checkpoint = newStart
                segment
            }.toList

        // create the last line segment
        val segment = geometryFactory.createLineString(Array(checkpoint, end))
        segment ::  segments
    }



    /**
     * Split polygons into smaller polygons. Splitting is determined by the threshold.
     * The width and the height of the produced polygons will not exceed the threshold
     *
     * @param polygon       input polygon
     * @return              a seq of smaller polygons
     */
    def decomposePolygon(polygon: Polygon): Seq[Geometry] = {

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
            val (bigPolygons, smallPolygons) = polygons.partition(p => p.getEnvelopeInternal.getWidth > theta.x || p.getEnvelopeInternal.getHeight > theta.y)
            val (widePolygons, nonWide) = bigPolygons.partition(p => p.getEnvelopeInternal.getWidth > theta.x)
            val (tallPolygons, nonTall) = bigPolygons.partition(p => p.getEnvelopeInternal.getHeight > theta.y)
            if (widePolygons.nonEmpty) {
                val newPolygons = widePolygons.flatMap{ polygon =>
                    val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))
                    val verticalBlade = getVerticalBlade(polygon)
                    val blades = combineBladeWithInteriorRings(polygon, verticalBlade, innerRings, isHorizontal=false)
                    split(polygon,  blades)
                }
                recursiveSplit(newPolygons ++ nonWide, smallPolygons ++ accumulator )
            }
            else if (tallPolygons.nonEmpty) {
                val newPolygons = tallPolygons.flatMap{ polygon =>
                    val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))
                    val horizontalBlade = getHorizontalBlade(polygon)
                    val blades = combineBladeWithInteriorRings(polygon, horizontalBlade, innerRings, isHorizontal=true)
                    split(polygon,  blades)
                }
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
        // apply
        recursiveSplit(List(polygon))
    }


    /**
     * Split polygons into smaller polygons considering only one dimension.
     *  Splitting is determined by the tile granularity.
     *  Either the width or the height of the produced polygons will not exceed the threshold
     *  Always split on the larget dimension (width or height) of the initial polygon
     *
     * @param polygon       input polygon
     * @return              a seq of smaller polygons
     */
    def decomposePolygon1D(polygon: Polygon): Seq[Geometry] = {

        /**
         * Recursively, split the polygons into sub-polygons. The procedure is repeated
         * until the width or height of the produced polygons do not exceed predefined thresholds.
         *
         * @param polygons      a list of Polygons
         * @param accumulator   the list of sub-polygons produced in the previous recursion
         * @param splitHorizontally split horizontally otherwise vertically
         * @param splitCondition the condition based on which we split (based on width or height)
         * @return A list of sub-polygons
         */
        @tailrec
        def recursiveSplit(polygons: Seq[Polygon], accumulator: Seq[Polygon], splitHorizontally: Boolean, splitCondition: Envelope => Boolean): Seq[Polygon] = {

            val (bigPolygons, restPolygons) = polygons.partition(p => splitCondition(p.getEnvelopeInternal))
            if (bigPolygons.nonEmpty) {
                val newPolygons = bigPolygons.flatMap{ polygon =>
                    val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))
                    val blades = if (splitHorizontally) getHorizontalBlade(polygon) else getVerticalBlade(polygon)
                    val adjustedBlades = combineBladeWithInteriorRings(polygon, blades, innerRings, isHorizontal=false)
                    split(polygon,  adjustedBlades)
                }
                recursiveSplit(newPolygons, restPolygons ++ accumulator, splitHorizontally, splitCondition)
            }
            else
                restPolygons ++ accumulator
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

        // apply
        val env = polygon.getEnvelopeInternal
        val splitHorizontally = env.getHeight > env.getWidth
        val splitCondition = if (env.getHeight > env.getWidth) (e: Envelope) => e.getHeight > theta.y
                             else (e: Envelope) => e.getWidth > theta.x
        recursiveSplit(List(polygon), Nil, splitHorizontally, splitCondition)
    }


    /**
     * Split linestring into smaller lineStrings. Splitting is determined by the threshold.
     * The width and the height of the produced lineStrings will not exceed the threshold
     *
     * @param line           input linestring
     * @return              a seq of smaller linestring
     */
    def decomposeLineString(line: LineString): Seq[Geometry] = {

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
            val (bigLines, smallLines) = lines.partition(l => l.getEnvelopeInternal.getWidth > theta.x || l.getEnvelopeInternal.getHeight > theta.y)
            val (wideLines, nonWide) = bigLines.partition(p => p.getEnvelopeInternal.getWidth > theta.x)
            val (tallLines, nonTall) = bigLines.partition(p => p.getEnvelopeInternal.getHeight > theta.y)

            if (wideLines.nonEmpty) {
                val newLines = wideLines.flatMap(l => split(l, getVerticalBlade(l)))
                recursiveSplit(newLines ++ nonWide, smallLines ++ accumulator )
            }
            else if (tallLines.nonEmpty) {
                val newLines = tallLines.flatMap(l => split(l, getHorizontalBlade(l)))
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
            val lineSegments = line.difference(blade).asInstanceOf[MultiLineString]
            val results = (0 until lineSegments.getNumGeometries).map(i => lineSegments.getGeometryN(i).asInstanceOf[LineString])
            results
        }

        // apply
        recursiveSplit(Seq(line), Nil)
    }


}
