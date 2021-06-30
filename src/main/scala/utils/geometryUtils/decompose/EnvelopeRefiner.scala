package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom.{Envelope, Geometry, LineString, Polygon}
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
            val xTimesAdjusted = math.ceil((1/log(xTimes+1, SPLIT_LOG_BASE)) * xTimes)
            val xRatio = xTimes/xTimesAdjusted


            val yTimes = ceil(env.getHeight / theta.y).toInt
            val yTimesAdjusted = math.ceil((1/log(yTimes+1, SPLIT_LOG_BASE)) * yTimes)
            val yRatio = yTimes/yTimesAdjusted

            theta * (xRatio, yRatio)
        }

        val env = geom.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) {
            val splitTheta = getSplitGranularity(env)

            val fineGrainedEnvelopes = refineEnvelope(geom, splitTheta)
            // TODO Clean
            //            geom match {
            //                case _: Polygon =>
            //                    var innerMaxX = fineGrainedEnvelopes.head.getMinX
            //                    var innerMinX = fineGrainedEnvelopes.head.getMaxX
            //                    var innerMaxY = fineGrainedEnvelopes.head.getMinY
            //                    var innerMinY = fineGrainedEnvelopes.head.getMaxY
            //
            //                    fineGrainedEnvelopes.tail.foreach { e =>
            //                        if (e.getMinX > innerMaxX) innerMaxX = e.getMinX
            //                        if (e.getMaxX < innerMinX) innerMinX = e.getMaxX
            //                        if (e.getMinY > innerMaxY) innerMaxY = e.getMinY
            //                        if (e.getMaxY < innerMinY) innerMinY = e.getMaxY
            //                    }
            //                    val innerEnv = new Envelope(innerMinX, innerMaxX, innerMinY, innerMaxY)
            //                    innerEnv :: fineGrainedEnvelopes
            //                case _ =>
            //                    fineGrainedEnvelopes
            //            }
            fineGrainedEnvelopes
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

        val verticalPointsSeq: Seq[Double] = GeometryUtils.getCenterPoints(List(env.getMinX, env.getMaxX), theta.x)
        val verticalPoints: SortedSet[Double] = collection.SortedSet(verticalPointsSeq: _*)
        val horizontalPointsSeq: Seq[Double] = GeometryUtils.getCenterPoints(List(env.getMinY, env.getMaxY), theta.y)
        val horizontalPoints: SortedSet[Double] = collection.SortedSet(horizontalPointsSeq: _*)

        // TODO Clean
        //        val verticalBlades: Seq[LineString] = verticalPointsSeq
        //            .map(x => Array(new Coordinate(x, env.getMinY-epsilon), new Coordinate(x, env.getMaxY+epsilon)))
        //            .map(coords => GeometryUtils.geomFactory.createLineString(coords))
        //        val horizontalBlades: Seq[LineString] = horizontalPointsSeq
        //            .map(y => Array(new Coordinate(env.getMinX-epsilon, y), new Coordinate(env.getMaxX+epsilon, y)))
        //            .map(coords => GeometryUtils.geomFactory.createLineString(coords))
        //        val midPoints = new ListBuffer[String]()

        val regionsConditions: Array[((Double, Double) => Boolean, Int)] = (
            for (x <- verticalPoints.sliding(2);
                 y <- horizontalPoints.sliding(2)) yield {
                (x1: Double, y1: Double) => x1 >= x.head && x1 <= x.last && y1 >= y.head && y1 <= y.last
            }).zipWithIndex.toArray

        val envelopes: Array[MBR] = Array.fill(regionsConditions.length)(MBR())
        for (c <- geom.getCoordinates.sliding(2)){
            val c1 = c.head
            val c2 = c.last

            val (maxX, minX) = if (c1.x > c2.x) (c1.x, c2.x) else (c2.x, c1.x)
            val vp = verticalPoints.from(minX).to(maxX)
            val intersectingVerticalPoints = GeometryUtils.getIntersectionWithVerticalLine(c1, c2, vp).toList

            val (maxY, minY) = if (c1.y > c2.y) (c1.y, c2.y) else (c2.y, c1.y)
            val hp = horizontalPoints.from(minY).to(maxY)
            val intersectingHorizontalPoints = GeometryUtils.getIntersectionWithHorizontalLine(c1, c2, hp).toList

            // TODO Clean
            //            midPoints.appendAll(intersectingHorizontalPoints.map(c => s"POINT (${c.x} ${c.y} )"))
            //            midPoints.appendAll(intersectingVerticalPoints.map(c => s"POINT (${c.x} ${c.y} )"))


            val allPoints = c1 +: (intersectingHorizontalPoints ::: intersectingVerticalPoints) :+ c2
            for (p <- allPoints){
                regionsConditions.filter{case (cond, _) => cond(p.x, p.y)}
                    .foreach{ case (_, i) => envelopes(i).update(p.x, p.y)}
            }
        }
        envelopes.filter(! _.isEmpty).map(e => e.getEnvelope).toList
    }

}
