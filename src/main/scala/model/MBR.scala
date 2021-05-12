package model

import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import utils.Constants.Relation
import utils.Constants.Relation.Relation

import scala.math._

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

/**
 * Minimum Bounding Box
 *
 */
case class MBR(env: Envelope){

    val maxX:Double = env.getMaxX
    val minX:Double = env.getMinX
    val maxY:Double = env.getMaxY
    val minY:Double = env.getMinY

    /**
     * return true if the reference point is in the block.
     * The reference point is the upper left point of their intersection
     *
     * @param mbr the mbr that intersects
     * @param b the examined block
     * @param tileGranularities tile granularities
     * @return true if the reference point is in the block
     */
    private[model]
    def referencePointFiltering(mbr:MBR, b:(Int, Int), tileGranularities: TileGranularities): Boolean ={

        val minX1 = minX / tileGranularities.x
        val minX2 = mbr.minX / tileGranularities.x
        val maxY1 = maxY / tileGranularities.y
        val maxY2 = mbr.maxY / tileGranularities.y

        val rf: (Double, Double) =(max(minX1, minX2), min(maxY1, maxY2))
        rf._1 < b._1 && rf._1+1 >= b._1 && rf._2 < b._2 && rf._2+1 >= b._2
    }

    /**
     * return true if the reference point is in the block and inside the partition
     * The reference point is the upper left point of their intersection
     *
     * @param mbr the mbr that intersects
     * @param b the examined block
     * @param tileGranularities tile granularities
     * @param partition the examining partition
     * @return  true if the reference point is in the block and in partition
     */
    private[model]
    def referencePointFiltering(mbr:MBR, b:(Int, Int), tileGranularities: TileGranularities, partition: MBR): Boolean ={

        val minX1 = minX / tileGranularities.x
        val minX2 = mbr.minX / tileGranularities.x
        val maxY1 = maxY / tileGranularities.y
        val maxY2 = mbr.maxY / tileGranularities.y

        val rf: (Double, Double) =(max(minX1, minX2)+.0000001, min(maxY1, maxY2)+.0000001)
        val blockContainsRF: Boolean =  b._1 <= rf._1 && b._1+1 >= rf._1 && b._2 <= rf._2 && b._2+1 >= rf._2
        blockContainsRF && partition.contains(rf)
    }

    private[model]
    def intersectingMBR(mbr:MBR, relation: Relation): Boolean =
        relation match {
            case Relation.CONTAINS | Relation.COVERS => contains(mbr)
            case Relation.WITHIN | Relation.COVEREDBY => within(mbr)
            case Relation.INTERSECTS | Relation.CROSSES | Relation.OVERLAPS | Relation.DE9IM=> intersects(mbr)
            case Relation.TOUCHES => touches(mbr)
            case Relation.DISJOINT => disjoint(mbr)
            case Relation.EQUALS => equals(mbr)
            case _ => false
        }


    /**
     *  check relation among MBRs
     *
     * @param mbr MBR to examine
     * @param relations requested relations
     * @return whether the relation is true
     */
    private[model]
    def intersectingMBR(mbr:MBR, relations: Seq[Relation]): Boolean = relations.exists { r => intersectingMBR(mbr, r) }

    def adjust(tileGranularities: TileGranularities) : MBR ={

        val maxX = this.maxX / tileGranularities.x
        val minX = this.minX / tileGranularities.x
        val maxY = this.maxY / tileGranularities.y
        val minY = this.minY / tileGranularities.y

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

    def getArea: Double = (maxX - minX) * (maxY - minY)

    override def toString: String = getGeometry.toText

    def getIntersectingInterior(mbr:MBR): MBR = MBR(min(maxX, mbr.maxX), max(minX, mbr.minX), min(maxY, mbr.maxY), max(minY, mbr.minY))

    private[model]
    def equals(mbr:MBR): Boolean = minX == mbr.minX && maxX == mbr.maxX && minY == mbr.minY && maxY == mbr.maxY

    private[model]
    def contains(mbr:MBR): Boolean = minX <= mbr.minX && maxX >= mbr.maxX && minY <= mbr.minY && maxY >= mbr.maxY

    private[model]
    def contains(minX: Double, maxX: Double, minY: Double, maxY: Double): Boolean = minX <= minX && maxX >= maxX && minY <= minY && maxY >= maxY

    private[model]
    def contains(c: (Double, Double)): Boolean = minX <= c._1 && maxX >= c._1 && minY <= c._2 && maxY >= c._2

    private[model]
    def within(mbr: MBR):Boolean = mbr.contains(this)

    private[model]
    def touches(mbr: MBR): Boolean = maxX == mbr.maxX || minX == mbr.minX || maxY == mbr.maxY || minY == mbr.minY

    private[model]
    def intersects(mbr:MBR): Boolean = ! disjoint(mbr)

    private[model]
    def disjoint(mbr:MBR): Boolean = minX > mbr.maxX || maxX < mbr.minX || minY > mbr.maxY || maxY < mbr.minY

}


object  MBR {

    def apply(geom: Geometry): MBR = MBR(geom.getEnvelopeInternal)

    def apply(maxX: Double, minX: Double, maxY: Double, minY: Double): MBR = MBR(new Envelope(minX, maxX, minY, maxY))

}
