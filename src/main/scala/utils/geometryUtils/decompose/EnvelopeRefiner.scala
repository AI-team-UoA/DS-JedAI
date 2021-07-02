package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, LineString, LinearRing, Polygon}
import utils.geometryUtils.GeometryUtils

import scala.collection.SortedSet
import scala.math.ceil


case class EnvelopeRefiner(theta: TileGranularities) extends GridDecomposerT[Envelope] {

    val SPLIT_LOG_BASE: Int = 50

    override def decomposePolygon(polygon: Polygon): Seq[Envelope] = getFineGrainedEnvelope(polygon)

    override def decomposeLineString(line: LineString): Seq[Envelope] = getFineGrainedEnvelope(line)

    override def decomposeGeometry(geometry: Geometry): Seq[Envelope] = getFineGrainedEnvelope(geometry)


    def getFineGrainedEnvelope(geom: Geometry): List[Envelope] ={

        def getSplitGranularity(env: Envelope): TileGranularities = {
            val log: (Int, Int) => Double = (x, b) => math.log10(x)/math.log10(b)

            val xTimes = ceil(env.getWidth / theta.x).toInt
            val xRatio =
                if (xTimes != 0){
                    val xTimesAdjusted = math.ceil((1/log(xTimes+1, SPLIT_LOG_BASE)) * xTimes)
                    xTimes/xTimesAdjusted
                }
                else 1

            val yTimes = ceil(env.getHeight / theta.y).toInt
            val yRatio =
                if (yTimes != 0){
                    val yTimesAdjusted = math.ceil((1/log(yTimes+1, SPLIT_LOG_BASE)) * yTimes)
                    yTimes/yTimesAdjusted
                }
                else 1

            theta * (xRatio, yRatio)
        }

        val env = geom.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) {
            val splitTheta = getSplitGranularity(env)
            refineEnvelope(geom, splitTheta)
        }
        else
            List(env)
    }

    def refineEnvelope(geom: Geometry, theta: TileGranularities): List[Envelope] ={
        case class MBR(var minX: Double = Double.PositiveInfinity, var maxX: Double = Double.NegativeInfinity,
                       var minY: Double = Double.PositiveInfinity, var maxY: Double = Double.NegativeInfinity){

            def update(x: Double, y: Double): Unit = {
                if (minX > x) minX = x
                if (maxX < x) maxX = x
                if (minY > y) minY = y
                if (maxY < y) maxY = y
            }
            def getEnvelope: Envelope = new Envelope(minX, maxX, minY, maxY)
            def getGeometry: Geometry = geometryFactory.toGeometry(getEnvelope)
            def isEmpty: Boolean = minX == Double.PositiveInfinity || maxX == Double.NegativeInfinity ||
                minY == Double.PositiveInfinity || maxY == Double.NegativeInfinity
        }

        val env: Envelope = geom.getEnvelopeInternal
        val (verticalPointsSeq,horizontalPointsSeq) =
        geom match {
            case _: LineString =>
                val vertical = env.getMinX +:getVerticalPoints(env, theta.x) :+ env.getMaxX
                val horizontal= env.getMinY +: getHorizontalPoints(env, theta.y) :+ env.getMaxY
                (vertical, horizontal)
            case _ if env.getWidth > env.getHeight =>
                // cut vertically
                val vertical = env.getMinX +:getVerticalPoints(env, theta.x) :+ env.getMaxX
                val horizontal = env.getMinY :: env.getMaxY :: Nil
                (vertical, horizontal)
            case _ =>
                // cut horizontally
                val vertical = env.getMinX :: env.getMaxX :: Nil
                val horizontal = env.getMinY +: getHorizontalPoints(env, theta.y) :+ env.getMaxY
                (vertical, horizontal)
        }
        val verticalPoints: SortedSet[Double] = collection.SortedSet(verticalPointsSeq: _*)
        val horizontalPoints: SortedSet[Double] = collection.SortedSet(horizontalPointsSeq: _*)

        val regionsConditions: Array[((Double, Double) => Boolean, Int)] = (
            for (x <- verticalPoints.sliding(2);
                 y <- horizontalPoints.sliding(2)) yield {
                (x1: Double, y1: Double) => x1 >= x.head && x1 <= x.last && y1 >= y.head && y1 <= y.last
            }).zipWithIndex.toArray

        val envelopes: Array[MBR] = Array.fill(regionsConditions.length)(MBR())
        geom match {
            case polygon: Polygon =>
                val exteriorRing: LinearRing = polygon.getExteriorRing
                val interiorRings: Seq[LinearRing] = for (i <- 0 until polygon.getNumInteriorRing) yield polygon.getInteriorRingN(i)
                for (ring <- exteriorRing +: interiorRings) {
                    for (c <- ring.getCoordinates.sliding(2)) {
                        val c1 = c.head
                        val c2 = c.last
                        val intermediatePoints = findIntermediatePoints(c1, c2, verticalPoints, horizontalPoints)
                        for (p <- c1 +: intermediatePoints :+ c2) {
                            regionsConditions.filter { case (cond, _) => cond(p.x, p.y) }
                                .foreach { case (_, i) => envelopes(i).update(p.x, p.y) }
                        }
                    }
                }
            case _ =>
                for (c <- geom.getCoordinates.sliding(2)){
                    val c1 = c.head
                    val c2 = c.last
                    val intermediatePoints = findIntermediatePoints(c1, c2, verticalPoints, horizontalPoints)
                    for (p <- c1 +: intermediatePoints :+ c2){
                        regionsConditions.filter{case (cond, _) => cond(p.x, p.y)}
                            .foreach{ case (_, i) => envelopes(i).update(p.x, p.y)}
                    }
                }
        }

        envelopes.filter(! _.isEmpty).map(e => e.getEnvelope).toList
    }


    def findIntermediatePoints(c1: Coordinate, c2: Coordinate, verticalPoints: SortedSet[Double], horizontalPoints: SortedSet[Double]): List[Coordinate] ={
        val (maxX, minX) = if (c1.x > c2.x) (c1.x, c2.x) else (c2.x, c1.x)
        val vp = verticalPoints.from(minX).to(maxX)
        val intersectingVerticalPoints = GeometryUtils.getIntersectionWithVerticalLine(c1, c2, vp).toList

        val (maxY, minY) = if (c1.y > c2.y) (c1.y, c2.y) else (c2.y, c1.y)
        val hp = horizontalPoints.from(minY).to(maxY)
        val intersectingHorizontalPoints = GeometryUtils.getIntersectionWithHorizontalLine(c1, c2, hp).toList
        intersectingHorizontalPoints ::: intersectingVerticalPoints
    }

}
