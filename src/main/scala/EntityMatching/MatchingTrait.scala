package EntityMatching

import DataStructures.MBB
import com.vividsolutions.jts.geom.Geometry
import utils.Constants

trait MatchingTrait extends Serializable{

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


    def normalizeWeight(weight: Double, entity1: Geometry, entity2:Geometry): Double ={
        val area1 = entity1.getArea
        val area2 = entity2.getArea
        if (area1 == 0 || area2 == 0 ) weight
        else weight/(entity1.getArea * entity2.getArea)
    }


}
