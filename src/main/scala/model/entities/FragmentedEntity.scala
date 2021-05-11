package model.entities

import model.MBR
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import utils.Constants.Relation.Relation

case class FragmentedEntity(originalID: String = "", geometry: Geometry, mbr: MBR,
                            fragments: Seq[(Geometry, MBR)]) extends Entity {

    override def testMBR(e: Entity, relation: Relation*): Boolean =
        e match {
            case fe: FragmentedEntity =>
                mbr.testMBR(e.mbr, relation) && fragments.exists{ case (_, mbr1) => fe.fragments.exists{ case(_, mbr2) => mbr1.testMBR(mbr2, relation)} }
            case _ => mbr.testMBR(e.mbr, relation) && fragments.exists(fg => fg._2.testMBR(e.mbr, relation))
        }
}

object FragmentedEntity {

    def apply(e: Entity)(f: Geometry => Seq[Geometry]): FragmentedEntity ={
        val geometryFragments = f(e.geometry)
        FragmentedEntity(e.originalID, e.geometry, e.mbr, geometryFragments.map(gf => (gf, MBR(gf))))
    }

    def apply(originalID: String, wkt: String)(f: Geometry => Seq[Geometry]): FragmentedEntity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val mbr = MBR(geometry)
        val fragments = f(geometry).map(gf => (gf, MBR(gf)))

        FragmentedEntity(originalID, geometry, mbr, fragments)
    }

    def apply(originalID: String, geom: Geometry)(f: Geometry => Seq[Geometry]): FragmentedEntity ={
        val geometry: Geometry = geom
        val mbr = MBR(geometry)
        val fragments = f(geometry).map(gf => (gf, MBR(gf)))

        FragmentedEntity(originalID, geometry, mbr, fragments)
    }
}
