package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom._
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.geometryUtils.GeometryUtils.flattenCollection

import scala.annotation.tailrec
import collection.JavaConverters._

case class RecursiveDecomposer(theta: TileGranularities) extends DecomposerT[Geometry] {

    def decomposeGeometry(geometry: Geometry): Seq[Geometry] = {
        geometry match {
            case polygon: Polygon => decomposePolygon(polygon)
            case line: LineString => decomposeLineString(line)
            case gc: GeometryCollection => flattenCollection(gc).flatMap(g => decomposeGeometry(g))
            case _ => Seq(geometry)
        }
    }


    def getVerticalBlade(geom: Geometry): LineString = getVerticalBlade(geom.getEnvelopeInternal)

    def getHorizontalBlade(geom: Geometry): LineString = getHorizontalBlade(geom.getEnvelopeInternal)

    def getVerticalBlade(env: Envelope): LineString = {
        val x = (env.getMaxX + env.getMinX)/2
        val start = new Coordinate(x, env.getMinY-epsilon)
        val end = new Coordinate(x, env.getMaxY+epsilon)
        geometryFactory.createLineString(Array(start, end))
    }

    def getHorizontalBlade(env: Envelope): LineString = {
        val y = (env.getMaxY + env.getMinY)/2
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
     * @param blade the blade to combine with the inner holes
     * @param innerRings the inner holes as a sequence of inner lineRings
     * @param isHorizontal the requested blade is horizontal otherwise it will be vertical
     * @return
     */
    def combineBladeWithInteriorRings(blade: LineString, innerRings: Seq[Geometry], isHorizontal: Boolean): List[LineString] = {

        // ordering of the coordinates, to define max and min
        implicit val ordering: Ordering[Coordinate] = Ordering.by[Coordinate, Double](c => if (isHorizontal) c.x else c.y)

        // epsilon is a small value to add in the segments so to slightly intersect thus not result to dangling lines
        val (xEpsilon, yEpsilon) = if (isHorizontal) xYEpsilon else xYEpsilon.swap

        // the blade is a linestring formed by two points
        val start = blade.getCoordinateN(0)
        val end = blade.getCoordinateN(1)

        // define the cross condition based on line's orientation
        val crossCondition: Envelope => Boolean = env =>
            if (isHorizontal)
                (start.y >= env.getMinY && end.y <= env.getMaxY) && (start.x < env.getMinX && end.x > env.getMaxX)
            else
                (start.x >= env.getMinX && end.x <= env.getMaxX) && (start.y < env.getMinY && end.y > env.getMaxY)

        // sort inner rings envelope by y
        // find the ones that intersect with the line
        // for each intersecting inner ring find the intersection coordinates,
        //   - sort them and get the first and the last,
        //   - create line segments that do not overlap the inner ring
        var checkpoint = start
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



        val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))

        /**
         * Detect the bing envelopes and for each one create the splitting blades.
         * Repeat recursively with the produced envelopes after the split
         *
         * @param envelopes List of envelopes
         * @param accBlades returned blades
         * @return linestring
         */
        @tailrec
        def getBlades(envelopes: List[Envelope], accBlades: List[LineString]): List[LineString] = {
            val (bigEnvelopes, _) = envelopes.partition(env => env.getWidth > theta.x || env.getHeight > theta.y)
            val (wideEnvs, _) = bigEnvelopes.partition(p => p.getWidth > theta.x)
            val (tallEnvs, _) = bigEnvelopes.partition(p => p.getHeight > theta.y)
            if (wideEnvs.nonEmpty) {
                // find the vertical blades
                val verticalBlades: List[LineString] = wideEnvs.flatMap{ env =>
                    val verticalBlade = getVerticalBlade(env)
                    combineBladeWithInteriorRings(verticalBlade, innerRings, isHorizontal=false)
                }
                // find the envelopes, left and right from the blades
                val newEnvelopes = wideEnvs.flatMap{env =>
                    val meanX = (env.getMaxX + env.getMinX)/2
                    val leftEnv = new Envelope(env.getMinX, meanX, env.getMinY, env.getMaxY)
                    val rightEnv= new Envelope(meanX, env.getMaxX, env.getMinY, env.getMaxY)
                    leftEnv :: rightEnv :: Nil
                }
                getBlades(newEnvelopes, verticalBlades ++ accBlades )
            }
            else if (tallEnvs.nonEmpty) {
                // find the horizontal blades
                val horizontalBlades = tallEnvs.flatMap { env =>
                    val horizontalBlade = getHorizontalBlade(env)
                    combineBladeWithInteriorRings(horizontalBlade, innerRings, isHorizontal = true)
                }
                // find the envelopes, top and bottom from the blades
                val newEnvelopes = wideEnvs.flatMap{env =>
                    val meanY = (env.getMaxY + env.getMinY)/2
                    val bottomEnv = new Envelope(env.getMinX, env.getMaxX, env.getMinY, meanY)
                    val topEnv= new Envelope(env.getMinX, env.getMaxX, meanY, env.getMaxY)
                    bottomEnv :: topEnv :: Nil
                }
                getBlades(newEnvelopes, horizontalBlades ++ accBlades )
            }
            else
                accBlades
        }

        /**
         * Split a polygon using the input blade
         *
         * @param polygon input polygon
         * @param blades line segments that divide the polygon
         * @return a Seq of sub-polygons
         */
        def split(polygon: Polygon, blades: Seq[LineString]): Seq[Polygon] ={
            val exteriorRing = polygon.getExteriorRing
            val innerGeom: Seq[Geometry] = blades ++ innerRings
            val union = new UnaryUnionOp(innerGeom.asJava).union()
            val polygonizer = new Polygonizer()
            polygonizer.add(exteriorRing.union(union))
            val newPolygons = polygonizer.getPolygons.asScala.map(p => p.asInstanceOf[Polygon])
            newPolygons.filter(p => polygon.contains(p.getInteriorPoint)).toSeq
        }

        val blades = getBlades(polygon.getEnvelopeInternal:: Nil, Nil)
        if (blades.nonEmpty) split(polygon, blades) else List(polygon)
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
