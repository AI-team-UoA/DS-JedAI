package EntityMatching

import Blocking.BlockUtils
import DataStructures.{Block, MBB}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable.ArrayBuffer

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

/**
 *  Link discovery
 */
object Matching {

	/**
	 * check the relation between two geometries
	 *
	 * @param sourceGeom geometry from source set
	 * @param targetGeometry geometry from target set
	 * @param relation requested relation
	 * @return whether the relation is true
	 */
	def relate(sourceGeom: Geometry, targetGeometry: Geometry, relation: String): Boolean ={
		relation match {
			case Constants.CONTAINS => sourceGeom.contains(targetGeometry)
			case Constants.INTERSECTS => sourceGeom.intersects(targetGeometry)
			case Constants.CROSSES => sourceGeom.crosses(targetGeometry)
			case Constants.COVERS => sourceGeom.covers(targetGeometry)
			case Constants.COVEREDBY => sourceGeom.coveredBy(targetGeometry)
			case Constants.OVERLAPS => sourceGeom.overlaps(targetGeometry)
			case Constants.TOUCHES => sourceGeom.touches(targetGeometry)
			case Constants.DISJOINT => sourceGeom.disjoint(targetGeometry)
			case Constants.EQUALS => sourceGeom.equals(targetGeometry)
			case Constants.WITHIN => sourceGeom.within(targetGeometry)
			case _ => false
		}
	}

	/**
	 *  check relation among MBBs
	 *
	 * @param s MBB from source
	 * @param t MBB form target
	 * @param relation requested relation
	 * @return whether the relation is true
	 */
	def testMBB(s:MBB, t:MBB, relation: String): Boolean ={
		relation match {
			case Constants.CONTAINS | Constants.COVERS =>
				s.contains(t)
			case Constants.WITHIN | Constants.COVEREDBY =>
				s.within(t)
			case Constants.INTERSECTS | Constants.CROSSES | Constants.OVERLAPS =>
				s.intersects(t)
			case Constants.TOUCHES => s.touches(t)
			case Constants.DISJOINT => s.disjoint(t)
			case Constants.EQUALS => s.equals(t)
			case _ => false
		}
	}


	/**
	 * Perform the comparisons of the blocks. Only the comparisons inside the allowedComparison
	 * will be calculated.
	 *
	 * @param blocks RDD of blocks
	 * @param allowedComparisosn allowed comparisons per Block - RDD[(blockID, Array[comparisonID])]
 	 * @param relation requested relation
	 * @return the matches
	 */
	def SpatialMatching(blocks: RDD[Block], allowedComparisosn: RDD[(Int, Array[Int])], relation: String): RDD[(Int,Int)] ={

		val blocksComparisons = blocks.map(b => (b.id, (b.sourceSet, b.targetSet)))
		val matches = blocksComparisons.leftOuterJoin(allowedComparisosn)
    		.filter(_._2._2.isDefined)
    		.map { b =>
				val sourceSet = b._2._1._1
				val targetSet = b._2._1._2
				val allowedComparisons = b._2._2.get

				val matches: ArrayBuffer[(Int,Int)] = ArrayBuffer()
				for (s <- sourceSet.toIterator; t <- targetSet.toIterator){
					val comparisonID = BlockUtils.bijectivePairing(s.id, t.id)
					if (allowedComparisons.contains(comparisonID)){
						if (testMBB(s.mbb, t.mbb, relation))
						// TODO meridian case
						// TODO: append ids or originalIDs ?
							if (relate(s.geometry, t.geometry, relation))
								matches.append((s.id, t.id))
					}
				}
				matches
			}
    		.flatMap(a => a)
			matches
	}
}
