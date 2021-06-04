

import model.entities.{IndexedFragmentedEntity, SpatialEntity}
import model.{IM, SpatialIndex, TileGranularities}
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader
import org.scalatest.wordspec.AnyWordSpec
import utils.Constants.ThetaOption

class IntersectionMatrixTest extends AnyWordSpec  {

    val wktReader = new WKTReader()
    val geometryFactory = new GeometryFactory()

    val source: Seq[SpatialEntity] = TestingGeometries.source.zipWithIndex.map{ case (g, i) => SpatialEntity(i.toString, g)}
    val target: Seq[SpatialEntity] = TestingGeometries.target.zipWithIndex.map{ case (g, i) => SpatialEntity(i.toString, g)}

    val theta: TileGranularities = TileGranularities(source.map(_.env), source.length, ThetaOption.AVG_x2)
    val theta_ : TileGranularities = theta * (1/2)
    val fSource: Seq[IndexedFragmentedEntity] = source.map(e => IndexedFragmentedEntity(e, theta))
    val fTarget: Seq[IndexedFragmentedEntity] = target.map(e => IndexedFragmentedEntity(e, theta))

    val index: SpatialIndex[IndexedFragmentedEntity] = SpatialIndex[IndexedFragmentedEntity](fSource.toArray, theta)

    "IndexedFragmentedEntities" should {
        "produce correct IM" in {
            for (t <- fTarget; c <- index.getCandidates(t)) yield {

                val im = c.geometry.relate(t.geometry)
                val correctIM = IM(c, t, im)

                val fim = c.getIntersectionMatrix(t)
                assert(correctIM == fim)
            }
        }
    }


}
