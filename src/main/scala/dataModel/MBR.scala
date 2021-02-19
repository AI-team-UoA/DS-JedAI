package dataModel

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import utils.Constants.Relation
import utils.Constants.Relation.Relation

import scala.math._

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

/**
 * Minimum Bounding Box
 *
 * @param maxX max x
 * @param minX min x
 * @param maxY max y
 * @param minY min y
 */
case class MBR(maxX:Double, minX:Double, maxY:Double, minY:Double){


    /**
     * return true if the reference point is in the block.
     * The reference point is the upper left point of their intersection
     *
     * @param mbr the mbr that intersects
     * @param b the examined block
     * @param thetaXY blocks' granularity
     * @return true if the reference point is in the block
     */
    private[dataModel]
    def referencePointFiltering(mbr:MBR, b:(Int, Int), thetaXY: (Double, Double)): Boolean ={
        val (thetaX, thetaY) = thetaXY

        val minX1 = minX / thetaX
        val minX2 = mbr.minX / thetaX
        val maxY1 = maxY / thetaY
        val maxY2 = mbr.maxY / thetaY

        val rf: (Double, Double) =(max(minX1, minX2), min(maxY1, maxY2))
        rf._1 < b._1 && rf._1+1 >= b._1 && rf._2 < b._2 && rf._2+1 >= b._2
    }

    /**
     * return true if the reference point is in the block and inside the partition
     * The reference point is the upper left point of their intersection
     *
     * @param mbr the mbr that intersects
     * @param b the examined block
     * @param thetaXY blocks' granularity
     * @param partition the examining partition
     * @return  true if the reference point is in the block and in partition
     */
    private[dataModel]
    def referencePointFiltering(mbr:MBR, b:(Int, Int), thetaXY: (Double, Double), partition: MBR): Boolean ={
        val (thetaX, thetaY) = thetaXY

        val minX1 = minX / thetaX
        val minX2 = mbr.minX / thetaX
        val maxY1 = maxY / thetaY
        val maxY2 = mbr.maxY / thetaY

        val rf: (Double, Double) =(max(minX1, minX2)+.0000001, min(maxY1, maxY2)+.0000001)
        val blockContainsRF: Boolean =  b._1 <= rf._1 && b._1+1 >= rf._1 && b._2 <= rf._2 && b._2+1 >= rf._2
        blockContainsRF && partition.contains(rf)
    }


    /**
     *  check relation among MBRs
     *
     * @param mbr MBR to examine
     * @param relations requested relations
     * @return whether the relation is true
     */
    private[dataModel]
    def testMBR(mbr:MBR, relations: Seq[Relation]): Boolean =
        relations.map {
            case Relation.CONTAINS | Relation.COVERS =>
                contains(mbr)
            case Relation.WITHIN | Relation.COVEREDBY =>
                within(mbr)
            case Relation.INTERSECTS | Relation.CROSSES | Relation.OVERLAPS =>
                intersects(mbr)
            case Relation.TOUCHES => touches(mbr)
            case Relation.DISJOINT => disjoint(mbr)
            case Relation.EQUALS => equals(mbr)
            case Relation.DE9IM => intersects(mbr)
            case _ => false
        }.reduce( _ || _)

    def getIntersectingMBR(mbr:MBR): MBR = MBR(min(maxX, mbr.maxX), max(minX, mbr.minX), min(maxY, mbr.maxY), max(minY, mbr.minY))

    /**
     * check if the mbr is equal to the given one
     * @param mbr given mbr
     * @return whether it's true
     */
    private[dataModel]
    def equals(mbr:MBR): Boolean = minX == mbr.minX && maxX == mbr.maxX && minY == mbr.minY && maxY == mbr.maxY


    /**
     * check if the mbr contains the given one
     * @param mbr given mbr
     * @return whether it's true
     */
    private[dataModel]
    def contains(mbr:MBR): Boolean = minX <= mbr.minX && maxX >= mbr.maxX && minY <= mbr.minY && maxY >= mbr.maxY

    private[dataModel]
    def contains(minX: Double, maxX: Double, minY: Double, maxY: Double): Boolean = minX <= minX && maxX >= maxX && minY <= minY && maxY >= maxY


    private[dataModel]
    def contains(c: (Double, Double)): Boolean = minX <= c._1 && maxX >= c._1 && minY <= c._2 && maxY >= c._2


    /**
     * check if the mbr is within to the given one
     * @param mbr given mbr
     * @return whether it's true
     */
    private[dataModel]
    def within(mbr: MBR):Boolean = mbr.contains(this)


    /**
     * check if the mbr touches the given one
     * @param mbr given mbr
     * @return whether it's true
     */
    private[dataModel]
    def touches(mbr: MBR): Boolean = maxX == mbr.maxX || minX == mbr.minX || maxY == mbr.maxY || minY == mbr.minY


    /**
     * check if the mbr intersects the given one
     * @param mbr given mbr
     * @return whether it's true
     */
    private[dataModel]
    def intersects(mbr:MBR): Boolean = ! disjoint(mbr)


    /**
     * check if the mbr disjoints the given one
     * @param mbr given mbr
     * @return whether it's true
     */
    private[dataModel]
    def disjoint(mbr:MBR): Boolean = minX > mbr.maxX || maxX < mbr.minX || minY > mbr.maxY || maxY < mbr.minY


    def adjust(thetaXY: (Double, Double)) : MBR ={
        val (thetaX, thetaY) = thetaXY

        val maxX = this.maxX / thetaX
        val minX = this.minX / thetaX
        val maxY = this.maxY / thetaY
        val minY = this.minY / thetaY

        MBR(maxX, minX, maxY, minY)
    }


    /**
     * convert MBR into jts.Geometry
     * @return jts.Geometry
     */
    def getGeometry: Geometry = {
        val gf: GeometryFactory = new GeometryFactory()
        if (minX == maxX)
            gf.createPoint(new Coordinate(minX, minY))
        else {
            val coordsList: List[(Double, Double)] = List((minX, minY), (minX, maxY), (maxX, maxY), (maxX, minY), (minX, minY))
            val coordsAr: Array[Coordinate] = coordsList.map(c => new Coordinate(c._1, c._2)).toArray
            gf.createPolygon(coordsAr)
        }
    }

    def getArea: Float = ((maxX - minX) * (maxY - minY)).toFloat

    override def toString: String = "(" + minX.toString  + ", " + maxX.toString +"), ("+ minY.toString  + ", " + maxY.toString +")"

}



object  MBR {
    def apply(geom: Geometry): MBR ={
        val env = geom.getEnvelopeInternal
        MBR(env.getMaxX, env.getMinX, env.getMaxY, env.getMinY)
    }

    def apply(env: Envelope): MBR ={
        MBR(env.getMaxX, env.getMinX, env.getMaxY, env.getMinY)
    }

    def apply(x:Double, y:Double): MBR ={
       MBR(x, x, y, y)
    }
}
