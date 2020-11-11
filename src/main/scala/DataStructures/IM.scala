package DataStructures

case class IM(idPair: (String, String), isContains: Boolean, isCoveredBy: Boolean, isCovers: Boolean, isCrosses: Boolean,
              isEquals: Boolean, isIntersects: Boolean, isOverlaps: Boolean, isTouches: Boolean, isWithin: Boolean){

    lazy val relate: Boolean =
        isContains|| isCoveredBy || isCovers || isCrosses || isEquals || isIntersects || isOverlaps || isTouches || isWithin
}

object IM {
    def apply(e1: SpatialEntity, e2: SpatialEntity): IM = {
        val im = e1.getIntersectionMatrix(e2)
        val d1 = e1.geometry.getDimension
        val d2 = e2.geometry.getDimension
        IM((e1.originalID, e2.originalID), im.isContains, im.isCoveredBy, im.isCovers, im.isCrosses(d1, d2),
            im.isEquals(d1, d2), im.isIntersects, im.isOverlaps(d1, d2), im.isTouches(d1, d2), im.isWithin)
    }
}
