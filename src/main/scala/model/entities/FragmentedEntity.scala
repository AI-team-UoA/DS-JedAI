package model.entities

import model.IM
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import utils.Constants.Relation
import utils.Constants.Relation.Relation

case class FragmentedEntity(originalID: String = "", geometry: Geometry, fragments: Seq[Geometry]) extends Entity {

    override def intersectingMBR(e: Entity, relation: Relation): Boolean =
        e match {
            case fe: FragmentedEntity =>
                env.intersectingMBR(e.env, relation) &&
                    fragments.exists{ g1 =>
                        val env1 = g1.getEnvelopeInternal
                        fe.fragments.exists( g2 => env1.intersectingMBR(g2.getEnvelopeInternal, relation))
                    }
            case _ => env.intersectingMBR(e.env, relation) &&
                fragments.exists { fg =>
                    val env1 = fg.getEnvelopeInternal
                    env1.intersectingMBR(e.env, relation)
                }
        }

    def findIntersectingFragments(e: Entity):  Seq[(Geometry, Geometry)]=
        e match {
            case fe: FragmentedEntity =>
               for (f1 <- fragments;
                    f2 <- fe.fragments
                    if f1.getEnvelopeInternal.intersectingMBR(f2.getEnvelopeInternal, Relation.DE9IM)
                    ) yield (f1, f2)
            case _ =>
                for (f1 <- fragments
                     if f1.getEnvelopeInternal.intersectingMBR(e.env, Relation.DE9IM)
                     ) yield (f1, e.geometry)
        }

    override def getIntersectionMatrix(e: Entity): IM ={
        val fragmentsVerifications = findIntersectingFragments(e)
        val ims = fragmentsVerifications.map{case (f1, f2) =>  f1.relate(f2)}.map(im => IM(this, e, im))
        ims.reduce(_ + _)
    }
}

object FragmentedEntity {

    def apply(e: Entity)(f: Geometry => Seq[Geometry]): FragmentedEntity ={
        val geometryFragments = f(e.geometry)
        FragmentedEntity(e.originalID, e.geometry, geometryFragments)
    }

    def apply(originalID: String, wkt: String)(f: Geometry => Seq[Geometry]): FragmentedEntity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val fragments = f(geometry)

        FragmentedEntity(originalID, geometry, fragments)
    }

    def apply(originalID: String, geom: Geometry)(f: Geometry => Seq[Geometry]): FragmentedEntity ={
        val geometry: Geometry = geom
        val fragments = f(geometry)

        FragmentedEntity(originalID, geometry, fragments)
    }
}
