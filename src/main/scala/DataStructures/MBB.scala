package DataStructures

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import utils.Constants
import utils.Constants.Relation
import utils.Constants.Relation.Relation


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
case class MBB(maxX:Double, minX:Double, maxY:Double, minY:Double){


    /**
     * return true if the reference point is in the block.
     * The reference point is the upper left point of their intersection
     *
     * @param mbb the mbb that intersects
     * @param b the examined block
     * @return true if the reference point is in the block
     */
    def referencePointFiltering(mbb:MBB, b:(Int, Int), thetaXY: (Double, Double)): Boolean ={
        val (thetaX, thetaY) = thetaXY

        val minX1 = minX / thetaX
        val minX2 = mbb.minX / thetaX
        val maxY1 = maxY / thetaY
        val maxY2 = mbb.maxY / thetaY

        val rf: (Double, Double) = (math.max(minX1, minX2), math.min(maxY1, maxY2))
        rf._1 >= b._1 && rf._1 < b._1+1 && rf._2 >= b._2 & rf._2 < b._2+1

         // val rf: (Double, Double) = (math.max(minX1, minX2) + .5, math.min(maxY1, maxY2) + .5)
        // val p2 = rf._1 == b._1+1 || rf._2 == b._2+1
        // (p1 || p2) && (!p1 || !p2) // XOR
        // rf._1 >= b._1 && rf._1 < b._1+1 && rf._2 >= b._2 & rf._2 < b._2+1
    }

    /**
     *  check relation among MBBs
     *
     * @param mbb MBB to examine
     * @param relation requested relation
     * @return whether the relation is true
     */
    def testMBB(mbb:MBB, relation: Relation): Boolean ={
        relation match {
            case Relation.CONTAINS | Relation.COVERS =>
                contains(mbb)
            case Relation.WITHIN | Relation.COVEREDBY =>
                within(mbb)
            case Relation.INTERSECTS | Relation.CROSSES | Relation.OVERLAPS =>
                intersects(mbb)
            case Relation.TOUCHES => touches(mbb)
            case Relation.DISJOINT => disjoint(mbb)
            case Relation.EQUALS => equals(mbb)
            case _ => false
        }
    }

    /**
     * convert MBB into jts.Geometry
     * @return jts.Geometry
     */
    def getGeometry: Geometry ={
        val coordsList: List[(Double, Double)] = List((minX, minY), (minX, maxY), (maxX, maxY), (maxX, minY), (minX, minY))
        val coordsAr: Array[Coordinate] = coordsList.map(c => new Coordinate(c._1, c._2)).toArray
        val gf: GeometryFactory = new GeometryFactory()
        gf.createPolygon(coordsAr)
    }

    override def toString: String =
        "(" + minX.toString  + ", " + maxX.toString +"), ("+ minY.toString  + ", " + maxY.toString +")"

    /**
     * check if mbb crosses meridian
     * @return whether crosses meridian
     */
    def crossesMeridian: Boolean ={
        (minX < Constants.MAX_LONG && maxX > Constants.MAX_LONG) || (minX < Constants.MIN_LONG && maxX > Constants.MIN_LONG)
    }

    /**
     * Split mbb into two mbbs west and east of meridian
     * @return two mbbs west and east of meridian
     */
    def splitOnMeridian: (MBB, MBB) ={
        if (minX < Constants.MIN_LONG && maxX > Constants.MIN_LONG){
            val easternMBB: MBB = MBB(minX, Constants.MIN_LONG, maxY, minY)
            val westernMBB: MBB = MBB(Constants.MIN_LONG, maxX, maxY, minY)

            (westernMBB, easternMBB)
        }
        else if (minX < Constants.MAX_LONG && maxX >Constants.MAX_LONG) {
            val easternMBB: MBB = MBB(maxX, Constants.MAX_LONG, maxY, minY)
            val westernMBB: MBB = MBB(Constants.MAX_LONG, minX, maxY, minY)

            (westernMBB, easternMBB)
        }
        else (null, null)
    }

    /**
     * check if the mbb is equal to the given one
     * @param mbb given mbb
     * @return whether it's true
     */
    def equals(mbb:MBB): Boolean ={
        minX == mbb.minX && maxX == mbb.maxX && minY == mbb.minY && maxY == mbb.maxY
    }

    /**
     * check if the mbb contains the given one
     * @param mbb given mbb
     * @return whether it's true
     */
    def contains(mbb:MBB): Boolean ={
        minX <= mbb.minX && maxX >= mbb.maxX && minY <= mbb.minY && maxY >= mbb.maxY
    }

    /**
     * check if the mbb is within to the given one
     * @param mbb given mbb
     * @return whether it's true
     */
    def within(mbb: MBB):Boolean ={
        mbb.contains(this)
    }

    /**
     * check if the mbb touches the given one
     * @param mbb given mbb
     * @return whether it's true
     */
    def touches(mbb: MBB): Boolean ={
        maxX == mbb.maxX || minX == mbb.minX || maxY == mbb.maxY || minY == mbb.minY
    }

    /**
     * check if the mbb intersects the given one
     * @param mbb given mbb
     * @return whether it's true
     */
    def intersects(mbb:MBB): Boolean ={
        maxX > mbb.minX && minX < mbb.maxX && maxY > mbb.minY && minY < mbb.maxY
    }

    /**
     * check if the mbb disjoints the given one
     * @param mbb given mbb
     * @return whether it's true
     */
    def disjoint(mbb:MBB): Boolean ={
        ! (contains(mbb) || intersects(mbb))
    }

}

object  MBB {
    def apply(geom: Geometry): MBB ={
        val env = geom.getEnvelopeInternal
        MBB(env.getMaxX, env.getMinX, env.getMaxY, env.getMinY)
    }

    def apply(env: Envelope): MBB ={
        MBB(env.getMaxX, env.getMinX, env.getMaxY, env.getMinY)
    }
}
