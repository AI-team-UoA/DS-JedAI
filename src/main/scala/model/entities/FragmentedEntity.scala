package model.entities

import model.MBR
import org.locationtech.jts.geom.Geometry
import utils.Constants.Relation.Relation

case class FragmentedEntity(originalID: String = "", geometry: Geometry, mbr: MBR,
                            fragments: Seq[(Geometry, MBR)]) extends Entity {

    override def testMBR(e: Entity, relation: Relation*): Boolean =
        mbr.testMBR(e.mbr, relation) && fragments.exists(fg => fg._2.testMBR(mbr, relation))
}

object FragmentedEntity {
    def apply(e: Entity)(f: Geometry => Seq[Geometry]): FragmentedEntity ={
        val geometryFragments = f(e.geometry)
        FragmentedEntity(e.originalID, e.geometry, e.mbr, geometryFragments.map(gf => (gf, MBR(gf))))
    }
}
