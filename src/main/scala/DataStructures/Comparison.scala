package DataStructures

import utils.Utils

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class Comparison(id: Long, entity1: SpatialEntity, entity2: SpatialEntity)

object Comparison {
	def apply(se1: SpatialEntity, se2: SpatialEntity): Comparison ={
		Comparison(Utils.bijectivePairing(se1.id, se2.id), se1, se2)
	}
}
