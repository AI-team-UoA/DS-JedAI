package model

import model.entities.Entity
import org.apache.commons.math3.stat.inference.ChiSquareTest
import utils.Constants.WeightingFunction.WeightingFunction
import utils.Constants._

import scala.math.{ceil, floor, max, min}

sealed trait WeightedPair extends Serializable with Comparable[WeightedPair] {

    val counter: Int
    val entityId1: Int
    val entityId2: Int
    val mainWeight: Float
    val secondaryWeight: Float
    val typeWP: WeightingScheme

    var relatedMatches: Int = 0

    override def toString: String = s"${typeWP.value} WP s : $entityId1 t : $entityId2 main weight : $getMainWeight secondary weight : $getSecondaryWeight"

    /**
     * Returns the weight between two geometries. Higher weights indicate to
     * stronger likelihood of related entities.
     */
    def getMainWeight: Float = mainWeight * (1 + relatedMatches)

    def getSecondaryWeight: Float = secondaryWeight * (1 + relatedMatches)

    def incrementRelatedMatches(): Unit = relatedMatches += 1

    override def compareTo(o: WeightedPair): Int
}


/**
 *  In Main Weighted Pairs, we compare only using the main weight.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class MainWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float = 0f) extends WeightedPair {

    val typeWP: WeightingScheme = SINGLE

    override def compareTo(o: WeightedPair): Int = {
        val mwp = o.asInstanceOf[MainWP]

        if (entityId1 == mwp.entityId1 && entityId2 == mwp.entityId2) 0
        else {
            val diff = mwp.getMainWeight - getMainWeight
            if (0 < diff) 1
            else if (diff < 0) -1
            else mwp.counter - counter
        }
    }
}


/**
 *  In Composite Weighted Pairs, we use the secondary weight only as a resolver of ties.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class CompositeWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float) extends WeightedPair{

    val typeWP: WeightingScheme = COMPOSITE

    /**
     * Note: ID based comparison leads to violation of comparable contract
     * as may lead to cases that A > B, B > C and C > A.
     *
     * CompareTo will sort elements in a descendant order
     *
     * @param o a weighted pair
     * @return 1 if o is greater, 0 if they are equal, -1 if o is lesser.
     */
    override def compareTo(o: WeightedPair): Int = {

        val cwp = o.asInstanceOf[CompositeWP]

        if (entityId1 == cwp.entityId1 && entityId2 == cwp.entityId2) return 0

        val diff1 = cwp.getMainWeight - getMainWeight
        if (0 < diff1) return 1

        if (diff1 < 0) return -1

        val diff2 = cwp.getSecondaryWeight - getSecondaryWeight
        if (0 < diff2) return 1

        if (diff2 < 0) return -1

        cwp.counter - counter
    }
}


/**
 *  In Hybrid Weighted Pairs, the weight is determined by the product of the weights.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class HybridWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float) extends WeightedPair{

    val typeWP: WeightingScheme = HYBRID

    // the weights are not constant, so we recalculate the product
    def getProduct: Float = getMainWeight * getSecondaryWeight

    /**
     *
     * CompareTo will sort elements in a descendant order
     *
     * @param o a weighted pair
     * @return 1 if o is greater, 0 if they are equal, -1 if o is lesser.
     */
    override def compareTo(o: WeightedPair): Int = {

        val hwp = o.asInstanceOf[HybridWP]

        if (entityId1 == hwp.entityId1 && entityId2 == hwp.entityId2) 0
        else {
            val diff = hwp.getProduct - getProduct
            if (0 < diff) 1
            else if (diff < 0) -1
            else hwp.counter - counter
        }
    }
}

case class WeightedPairFactory(mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction],
                               weightingScheme: WeightingScheme, tileGranularities:TileGranularities, totalBlocks: Double) {

    /**
     * compute the main weight of a pair of entities
     * @param s source entity
     * @param t target entity
     * @return a weight
     */
    def getMainWeight(s: Entity, t: Entity): Float = getWeight(s, t, mainWF)


    /**
     * compute the secondary weight of a pair of entities, if the secondary scheme was provided
     * @param s source entity
     * @param t target entity
     * @return a weight
     */
    def getSecondaryWeight(s: Entity, t: Entity): Float =
        secondaryWF match {
            case Some(wf) => getWeight(s, t, wf)
            case None => 0f
        }

    /**
     *
     * Weight a pair
     * @param s        Spatial entity
     * @param t        Spatial entity
     * @return weight
     */
    def getWeight(s: Entity, t: Entity, wf: WeightingFunction): Float = {
        val sBlocks = (ceil(s.getMaxX/tileGranularities.x).toInt - floor(s.getMinX/tileGranularities.x).toInt + 1) *
            (ceil(s.getMaxY/tileGranularities.y).toInt - floor(s.getMinY/tileGranularities.y).toInt + 1)
        val tBlocks = (ceil(t.getMaxX/tileGranularities.x).toInt - floor(t.getMinX/tileGranularities.x).toInt + 1) *
            (ceil(t.getMaxY/tileGranularities.y).toInt - floor(t.getMinY/tileGranularities.y).toInt + 1)
        lazy val cb =
            (min(ceil(s.getMaxX/tileGranularities.x), ceil(t.getMaxX/tileGranularities.x)).toInt - max(floor(s.getMinX/tileGranularities.x),floor(t.getMinX/tileGranularities.x)).toInt + 1) *
            (min(ceil(s.getMaxY/tileGranularities.y), ceil(t.getMaxY/tileGranularities.y)).toInt - max(floor(s.getMinY/tileGranularities.y), floor(t.getMinY/tileGranularities.y)).toInt + 1)

        wf match {
            case WeightingFunction.MBRO =>
                val intersectionArea = s.getIntersectingInterior(t).getArea
                val w = intersectionArea / (s.env.getArea + t.env.getArea - intersectionArea)
                if (!w.isNaN) w.toFloat else 0f

            case WeightingFunction.ISP =>
                1f / (s.geometry.getNumPoints + t.geometry.getNumPoints);

            case WeightingFunction.JS =>
                cb / (sBlocks + tBlocks - cb)

            case WeightingFunction.PEARSON_X2 =>
                val v1: Array[Long] = Array[Long](cb, (tBlocks - cb).toLong)
                val v2: Array[Long] = Array[Long]((sBlocks - cb).toLong, (totalBlocks - (v1(0) + v1(1) + (sBlocks - cb))).toLong)
                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2)).toFloat

            case WeightingFunction.CF | _ =>
                cb.toFloat
        }
    }


    def createWeightedPair(counter: Int, s: Entity, sIndex: Int, t:Entity, tIndex: Int): WeightedPair = {
        weightingScheme match {
            case SINGLE =>
                val mw = getWeight(s, t, mainWF)
                MainWP(counter, sIndex, tIndex, mw)
            case COMPOSITE =>
                val mw = getWeight(s, t, mainWF)
                val sw = getSecondaryWeight(s, t)
                CompositeWP(counter, sIndex, tIndex, mw, sw)
            case HYBRID =>
                val mw = getWeight(s, t, mainWF)
                val sw = getSecondaryWeight(s, t)
                HybridWP(counter, sIndex, tIndex, mw, sw)
        }
    }
}
