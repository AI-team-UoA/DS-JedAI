import model.entities.{Entity, SpatialEntity}
import model.{SpatialIndex, TileGranularities}
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory, Polygon}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import utils.Constants.ThetaOption
import utils.decompose.GridFragmentation

class GridFragmentationTest  extends AnyFunSuite with Matchers {
    val wktReader = new WKTReader()
    val geomFactory = new GeometryFactory()
    val delta = 1e-10
    val lineT = 2e-1
    val polygonT = 5e-2

    // TODO handle case with holes
    test("GridFragmentation") {
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        val allPolygonsWKT = TestingGeometries.polygonsWKT
        val polygons: Seq[Polygon] = allPolygonsWKT.map(g => wktReader.read(g).asInstanceOf[Polygon])
        val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)

        val res = polygons.forall { p =>
            val subPolygons: Seq[Polygon] = GridFragmentation.splitPolygon(p, theta)
            val merged = subPolygons.foldLeft(emptyGeom)(_ union _)
            val diff = math.abs(merged.getArea - p.getArea)
            diff < delta
        }
        res shouldBe true
    }

    test("GridFragmentation - SpatialIndex") {
        val entities: Seq[Entity] = TestingGeometries.polygonsWKT
            .map(g => wktReader.read(g).asInstanceOf[Polygon])
            .zipWithIndex
            .map{case (p, i) => SpatialEntity(i.toString, p)}

        val theta = TileGranularities(entities.map(e => e.env), entities.length, ThetaOption.AVG_x2)
        val index = SpatialIndex(entities.toArray, theta)

        val res: Boolean = entities.forall { e =>
            val p = e.geometry.asInstanceOf[Polygon]
            val subPolygons: Seq[Polygon] = GridFragmentation.splitPolygon(p, theta)
            if (subPolygons.length == 1)
                true
            else {

                val tiles = index.indexEntity(e)
                //extend it with delta to avoid precision errors
                val tilesEnvelope = tiles.map(t =>
                    new Envelope(
                        (t._1 * theta.x) - delta,
                        ((t._1 + 1) * theta.x) + delta,
                        (t._2 * theta.y) - delta,
                        ((t._2 + 1) * theta.y) + delta
                    )
                ).map(e => geomFactory.toGeometry(e))

                val envelopes = subPolygons.map(p => p.getEnvelope)
                envelopes.forall { e =>
                    val covers = tilesEnvelope.filter(te => te.covers(e))
                    covers.length == 1
                }
            }
        }
        res shouldBe true
    }


}
