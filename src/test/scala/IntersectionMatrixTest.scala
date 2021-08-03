

import model.entities.{IndexedDecomposedEntity, SpatialEntity}
import model.{IM, SpatialIndex, TileGranularities}
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader
import org.scalatest.wordspec.AnyWordSpec
import utils.configuration.Constants.ThetaOption
import utils.geometryUtils.decompose.GridDecomposer

class IntersectionMatrixTest extends AnyWordSpec  {

    val wktReader = new WKTReader()
    val geometryFactory = new GeometryFactory()

    val source: Seq[SpatialEntity] = TestingGeometries.source.zipWithIndex.map{ case (g, i) => SpatialEntity(i.toString, g)}
    val target: Seq[SpatialEntity] = TestingGeometries.target.zipWithIndex.map{ case (g, i) => SpatialEntity(i.toString, g)}

    val theta: TileGranularities = TileGranularities(source.map(_.env), source.length, ThetaOption.AVG_x2)
    val decomposer: GridDecomposer = GridDecomposer(theta)
    val segmentationF: Geometry => Seq[Geometry] = decomposer.decomposeGeometry
    val fSource: Seq[IndexedDecomposedEntity] = source.map(e => IndexedDecomposedEntity(e, theta, segmentationF))
    val fTarget: Seq[IndexedDecomposedEntity] = target.map(e => IndexedDecomposedEntity(e, theta, segmentationF))

    val index: SpatialIndex[IndexedDecomposedEntity] = SpatialIndex[IndexedDecomposedEntity](fSource.toArray, theta)

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
