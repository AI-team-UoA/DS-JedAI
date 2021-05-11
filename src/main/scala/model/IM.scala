package model

import model.entities.Entity
import org.locationtech.jts.geom.IntersectionMatrix

case class IM(s: Entity, t: Entity, im: IntersectionMatrix){
    val idPair: (String, String) = (s.originalID, t.originalID)
    val relate: Boolean = !im.isDisjoint
    val isContains : Boolean = im.isContains
    val isCoveredBy : Boolean = im.isCoveredBy
    val isCovers : Boolean = im.isCovers
    val isCrosses : Boolean = im.isCrosses(s.geometry.getDimension, t.geometry.getDimension)
    val isEquals : Boolean = im.isEquals(s.geometry.getDimension, t.geometry.getDimension)
    val isIntersects : Boolean = im.isIntersects
    val isOverlaps : Boolean = im.isOverlaps(s.geometry.getDimension, t.geometry.getDimension)
    val isTouches : Boolean = im.isTouches(s.geometry.getDimension, t.geometry.getDimension)
    val isWithin : Boolean = im.isWithin

    def +(intersectionMatrix: IM): IM = {
        assert(intersectionMatrix.idPair == idPair)
        im.add(intersectionMatrix.im)
        this
    }
}

object IM {
    def apply(s: Entity, t: Entity): IM = {
        val im = s.getIntersectionMatrix(t)
        IM(s, t, im)
    }
}
