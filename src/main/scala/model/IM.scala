package model

case class IM(idPair: (String, String), isContains: Boolean, isCoveredBy: Boolean, isCovers: Boolean, isCrosses: Boolean,
              isEquals: Boolean, isIntersects: Boolean, isOverlaps: Boolean, isTouches: Boolean, isWithin: Boolean){

    lazy val relate: Boolean =
        isContains|| isCoveredBy || isCovers || isCrosses || isEquals || isIntersects || isOverlaps || isTouches || isWithin
}

object IM {
    def apply(s: Entity, t: Entity): IM = {
        val im = s.getIntersectionMatrix(t)
        val d1 = s.geometry.getDimension
        val d2 = t.geometry.getDimension
        IM((s.originalID, t.originalID), im.isContains, im.isCoveredBy, im.isCovers, im.isCrosses(d1, d2),
            im.isEquals(d1, d2), im.isIntersects, im.isOverlaps(d1, d2), im.isTouches(d1, d2), im.isWithin)
    }
}
