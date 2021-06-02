package model.entities

import model.{IM, SpatialIndex, TileGranularities}
import org.locationtech.jts.geom.{Geometry, IntersectionMatrix}
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp.EnvelopeIntersectionTypes
import utils.geometryUtils.EnvelopeOp.EnvelopeIntersectionTypes.EnvelopeIntersectionTypes
import utils.geometryUtils.{EnvelopeOp, GridFragmentation}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._

case class IndexedFragmentedEntity(originalID: String, geometry: Geometry, fragments: Array[Geometry], index: SpatialIndex[Geometry]) extends Entity {

    def getTileIndices: Set[(Int, Int)] = index.indices

    def getFragmentsFromTile(tile: (Int, Int)): Seq[Geometry] = index.get(tile)

    def getFragmentsIndexFromTile(tile: (Int, Int)): Seq[Int] = index.getIndices(tile)

    override def intersectingMBR(e: Entity, relation: Relation): Boolean = {
        lazy val fragmentsIntersection: Boolean = e match {

            case fe: IndexedFragmentedEntity =>
                index.indices.intersect(fe.getTileIndices).nonEmpty

            case fe: FragmentedEntity =>
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


    @tailrec
    private def ruleBasedVerification(pairs: List[(EnvelopeIntersectionTypes, (Geometry, Geometry))], im: IntersectionMatrix): IntersectionMatrix = {
        pairs match {
            case Nil => im
            case head :: tail =>
                val (envelopeIntersectionType, (g1, g2)) = head
                if(envelopeIntersectionType == EnvelopeIntersectionTypes.RANK0)
                    im
                else {
                    val newIm = g1.relate(g2)
                    im.add(newIm)
                    if (im.isCrosses(g1.getDimension, g2.getDimension) || im.isOverlaps(g1.getDimension, g2.getDimension))
                        im
                    else
                        ruleBasedVerification(tail, im)
                }
        }
    }


     override def getIntersectionMatrix(e: Entity): IM = {

         e match {
             case fe: IndexedFragmentedEntity =>

                 val intersectingTiles = index.indices.intersect(fe.getTileIndices).toSeq

                 // WARNING: the extra points introduced by the blades affect the results,
                 //  - consider to remove it
                 //  - or add the same point in fragment
                 val verifications: Seq[(Geometry, Geometry)] =

                     if (fe.fragments.length == 1){
//                         val intersectingFragments = intersectingTiles.flatMap{ t => getFragmentsFromTile(t)}
//                         val unifiedFragmentsTry = Try(UnaryUnionOp.union(intersectingFragments.asJava))
//                         unifiedFragmentsTry match {
//                             case Failure(_)                => intersectingFragments.map(g => (g, fe.geometry))
//                             case Success(unifiedFragments) => Seq((unifiedFragments, fe.geometry))
//                         }
                         Seq((geometry, fe.geometry))

                     }
                     else if (fragments.length == 1){
//                         val intersectingFragments = intersectingTiles.flatMap{ t => fe.getFragmentsFromTile(t)}
//                         val unifiedFragmentsTry = Try(UnaryUnionOp.union(intersectingFragments.asJava))
//                         unifiedFragmentsTry match {
//                             case Failure(_)                => intersectingFragments.map(g => (geometry, g))
//                             case Success(unifiedFragments) => Seq((geometry, unifiedFragments))
//                         }
                         Seq((geometry, fe.geometry))

                     }
                     else {
                         intersectingTiles
                             .map(t => (getFragmentsIndexFromTile(t), fe.getFragmentsIndexFromTile(t)))
                             .flatMap { case (indices1, indices2) => for (i <- indices1; j <- indices2) yield (i, j) }
                             .map{ case (i, j) => (fragments(i), fe.fragments(j))}
                     }

                 val typedVerifications: List[(EnvelopeIntersectionTypes, (Geometry, Geometry))] =
                     verifications.map { case (g1, g2) =>
                         val envelopeIntersectionType = EnvelopeOp.getIntersectingEnvelopesType(g1.getEnvelopeInternal, g2.getEnvelopeInternal)
                         (envelopeIntersectionType, (g1, g2))
                     }.sortBy(_._1).toList

                 val emptyIM = new IntersectionMatrix("FFFFFFFFF")
                 val im = ruleBasedVerification(typedVerifications, emptyIM)

//                 val im1 = IM(this, fe, im)
//                 val imGT = geometry.relate(fe.geometry)
//                 val im2 = IM(this, fe, imGT)
//                 if( im1 != im2){
//                     val emptyIM = new IntersectionMatrix("FFFFFFFFF")
//                     val m = ruleBasedVerification(typedVerifications, emptyIM)
//                     val k = 2
//                 }

                 IM(this, fe, im)

             case e: Entity => super.getIntersectionMatrix(e)
         }
     }
}


object IndexedFragmentedEntity{

    def apply(e: Entity, theta: TileGranularities): IndexedFragmentedEntity ={
        val geometryFragments = GridFragmentation.splitBigGeometries(theta)(e.geometry).toArray
        val index = SpatialIndex(geometryFragments, theta)
        IndexedFragmentedEntity(e.originalID, e.geometry, geometryFragments, index)
    }
}
