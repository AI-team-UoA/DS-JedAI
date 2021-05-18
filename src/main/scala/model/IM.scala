package model

import model.entities.Entity
import org.locationtech.jts.geom.IntersectionMatrix

case class IM(s: Entity, t: Entity, im: IntersectionMatrix){

    def getId1: String = s.originalID
    def getId2: String = t.originalID
    def relate: Boolean = !im.isDisjoint
    def isContains : Boolean = im.isContains
    def isCoveredBy : Boolean = im.isCoveredBy
    def isCovers : Boolean = im.isCovers
    def isCrosses : Boolean = im.isCrosses(s.geometry.getDimension, t.geometry.getDimension)
    def isEquals : Boolean = im.isEquals(s.geometry.getDimension, t.geometry.getDimension)
    def isIntersects : Boolean = im.isIntersects
    def isOverlaps : Boolean = im.isOverlaps(s.geometry.getDimension, t.geometry.getDimension)
    def isTouches : Boolean = im.isTouches(s.geometry.getDimension, t.geometry.getDimension)
    def isWithin : Boolean = im.isWithin

    def +(intersectionMatrix: IM): IM = {
        assert(intersectionMatrix.getId1 == getId1 && intersectionMatrix.getId2 == getId2)
        im.add(intersectionMatrix.im)
        this
    }

    def ==(im: IM): Boolean ={
        relate == im.relate &&
            isContains == im.isContains && isCoveredBy == im.isCoveredBy && isCovers == im.isCovers &&
            isCrosses == im.isCrosses && isEquals == im.isEquals && isIntersects == im.isIntersects &&
            isOverlaps == im.isOverlaps && isTouches == im.isTouches && isWithin == im.isWithin
    }
}
