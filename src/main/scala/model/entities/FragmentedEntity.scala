package model.entities

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import utils.Constants.Relation
import utils.Constants.Relation.Relation
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

//    override def getIntersectionMatrix(e: Entity): IM ={
//        // WARNING still does not catch all cases: in case
//        //      env not Contains e.env but geometry Contains e.geometry
//        //      and the blade has passed through e.geometry
//        if(env.contains(e.env) || e.env.contains(env))
//            super.getIntersectionMatrix(e)
//        else {
//            val fragmentsVerifications = findIntersectingFragments(e)
//            val ims = fragmentsVerifications.map { case (f1, f2) => f1.relate(f2) }
//
//            val emptyImStr = if (fragments.exists(f => f.getEnvelopeInternal.disjoint(e.env))) "FF*FF*FF*" else "*********"
//            val combinedIM =  new IntersectionMatrix(emptyImStr)
//            ims.foreach(im => combinedIM add im)
//            val im = IM(this, e, combinedIM)
//            im
//        }
//    }
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
