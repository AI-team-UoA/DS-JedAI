package model.entities

import org.locationtech.jts.geom.Geometry
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp

case class FragmentedEntity(originalID: String = "", geometry: Geometry, fragments: Seq[Geometry]) extends Entity {

    override def intersectingMBR(e: Entity, relation: Relation): Boolean ={
        lazy val fragmentsIntersection: Boolean = e match {
            case fe: FragmentedEntity =>
                fragments.exists { fg1 =>
                    val fragmentEnv = fg1.getEnvelopeInternal
                    fe.fragments.exists(fg2 => EnvelopeOp.checkIntersection(fragmentEnv, fg2.getEnvelopeInternal, relation))
                }

            case fe: IndexedFragmentedEntity =>
                fragments.exists { fg1 =>
                    val fragmentEnv = fg1.getEnvelopeInternal
                    fe.fragments.exists(fg2 => EnvelopeOp.checkIntersection(fragmentEnv, fg2.getEnvelopeInternal, relation))
                }

            case _ =>
                fragments.exists { fg =>
                    val fragmentEnv = fg.getEnvelopeInternal
                    EnvelopeOp.checkIntersection(fragmentEnv, e.env, relation)
                }
        }
        val envIntersection: Boolean = EnvelopeOp.checkIntersection(env, e.env, relation)
        envIntersection && fragmentsIntersection
    }


    def findIntersectingFragments(e: Entity):  Seq[(Geometry, Geometry)]=
        e match {
            case fe: FragmentedEntity =>
               for (f1 <- fragments;
                    f2 <- fe.fragments
                    if EnvelopeOp.checkIntersection(f1.getEnvelopeInternal, f2.getEnvelopeInternal, Relation.DE9IM)
                    ) yield (f1, f2)

            case fe: IndexedFragmentedEntity =>
                for (f1 <- fragments;
                     f2 <- fe.fragments
                     if EnvelopeOp.checkIntersection(f1.getEnvelopeInternal, f2.getEnvelopeInternal, Relation.DE9IM)
                     ) yield (f1, f2)
            case _ =>
                for (f1 <- fragments
                     if EnvelopeOp.checkIntersection(f1.getEnvelopeInternal, e.env, Relation.DE9IM)
                     ) yield (f1, e.geometry)
        }
}

object FragmentedEntity {

    def apply(e: Entity, decompose: Geometry => Seq[Geometry]): FragmentedEntity ={
        val fragments = decompose(e.geometry)
        FragmentedEntity(e.originalID, e.geometry, fragments)
    }

    def apply(originalID: String, geom: Geometry, decompose: Geometry => Seq[Geometry]): FragmentedEntity ={
        val fragments = decompose(geom)
        FragmentedEntity(originalID, geom, fragments)
    }
}
