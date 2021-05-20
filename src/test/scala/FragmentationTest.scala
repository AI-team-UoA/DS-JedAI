

import model.{SpatialIndex, TileGranularities}
import model.entities.{Entity, SpatialEntity}
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory, LineString, Polygon}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import utils.Constants.ThetaOption
import utils.decompose.{GeometryUtils, GridFragmentation, RecursiveFragmentation}

class FragmentationTest extends AnyFunSuite with Matchers {

    val wktReader = new WKTReader()
    val geomFactory = new GeometryFactory()
    val delta = 1e-5
    val lineT = 2e-1
    val polygonT = 5e-2


    /***
     * RECURSIVE FRAGMENTATION
     */

    /**
     * Test Geometry Collection flattening
     */
    test("GeometryUtils.flattenCollection - GeometryCollections") {
        val polygons: Seq[Geometry] = TestingGeometries.geometryCollectionsWKT.map(g => wktReader.read(g))
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        val res = polygons.forall { gc =>
            val geometries: Seq[Geometry] = GeometryUtils.flattenCollection(gc)
            val merged = geometries.foldLeft(emptyGeom)(_ union _)
            val diff = math.abs(merged.getArea - gc.getArea)
            diff < delta
        }
        res shouldBe true
    }

    /**
     * Test simple geometries Split
     */
    test("RecursiveFragmentation.splitPolygon - Polygons") {
        val polygons: Seq[Polygon] = TestingGeometries.polygonsWKT.map(g => wktReader.read(g).asInstanceOf[Polygon])
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
        val res = polygons.forall { p =>
            val fragments: Seq[Polygon] = RecursiveFragmentation.splitPolygon(p, theta)
            val merged = fragments.foldLeft(emptyGeom)(_ union _)
            val diff = math.abs(merged.getArea - p.getArea)
            diff < delta
        }
        res shouldBe true
    }


    /**
     * Test geometries with Inner rings Split
     */
    test("RecursiveFragmentation.splitPolygon - Ring Polygons") {
        val polygons: Seq[Polygon] = TestingGeometries.ringPolygonsWKT.map(g => wktReader.read(g).asInstanceOf[Polygon])
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
        val res = polygons.forall { p =>
            val fragments: Seq[Polygon] = RecursiveFragmentation.splitPolygon(p, theta)
            val merged = fragments.foldLeft(emptyGeom)(_ union _)
            val diff = math.abs(merged.getArea - p.getArea)
            diff < delta
        }
        res shouldBe true
    }


    /**
     * Test lineSting split
     */
    test("RecursiveFragmentation.splitLines") {
        val lineStrings: Seq[LineString] = TestingGeometries.lineStringsWKT.map(g => wktReader.read(g).asInstanceOf[LineString])
        val theta = TileGranularities(lineStrings.map(p => p.getEnvelopeInternal), lineStrings.length, ThetaOption.AVG_x2)
        val res = lineStrings.forall { l =>
            val lineSegments: Seq[LineString] = RecursiveFragmentation.splitLineString(l, theta)
            val linesLength: Double = lineSegments.map(_.getLength).sum
            val diff = math.abs(linesLength - l.getLength)
            diff < delta
        }
        res shouldBe true
    }


    /**
     * Test splitBigGeometries
     */
    test("RecursiveFragmentation.splitBigGeometries") {
        val wkt = TestingGeometries.lineStringsWKT ++ TestingGeometries.geometryCollectionsWKT ++
                    TestingGeometries.polygonsWKT ++ TestingGeometries.polygonsWKT
        val geometries: Seq[Geometry] = wkt.map(g => wktReader.read(g))
        val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
        val res = geometries.forall { g =>
            val res: Seq[Geometry] = RecursiveFragmentation.splitBigGeometries(theta)(g)
            val gArea: Double = res.map(_.getArea).sum
            val diffArea = math.abs(gArea - g.getArea)
            diffArea < delta
        }
        res shouldBe true
    }





    /***
     * GRID FRAGMENTATION
     */


    test("GridFragmentation - Simple Polygons") {
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        val allPolygonsWKT = TestingGeometries.polygonsWKT
        val polygons: Seq[Polygon] = allPolygonsWKT.map(g => wktReader.read(g).asInstanceOf[Polygon])
        val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)

        val res = polygons.forall { p =>
            val fragments: Seq[Polygon] = GridFragmentation.splitPolygon(p, theta)
            val merged = fragments.foldLeft(emptyGeom)(_ union _)
            val diff = math.abs(merged.getArea - p.getArea)
            diff < delta
        }
        res shouldBe true
    }



    test("GridFragmentation - Polygons with Rings") {
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        val allPolygonsWKT = TestingGeometries.ringPolygonsWKT
        val polygons: Seq[Polygon] = allPolygonsWKT.map(g => wktReader.read(g).asInstanceOf[Polygon])
        val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)

        val res = polygons.forall { p =>
            val fragments: Seq[Polygon] = GridFragmentation.splitPolygon(p, theta)
            val merged = fragments.foldLeft(emptyGeom)(_ union _)
            val diff = math.abs(merged.getArea - p.getArea)
            diff < delta
        }
        res shouldBe true
    }



    /**
     * Test lineSting split
     */
    test("GridFragmentation.splitLines") {
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        val lineStrings: Seq[LineString] = TestingGeometries.lineStringsWKT.map(g => wktReader.read(g).asInstanceOf[LineString])
        val theta = TileGranularities(lineStrings.map(p => p.getEnvelopeInternal), lineStrings.length, ThetaOption.AVG_x2)

        assert(
            lineStrings.forall { l =>
                val lineSegments: Seq[LineString] = GridFragmentation.splitLineString(l, theta)
                val linesLength: Double = lineSegments.map(_.getLength).sum
                val diff = math.abs(linesLength - l.getLength)
                diff < delta
            }
        )
    }

    test("GridFragmentation - SpatialIndex") {
        val delta = 1e-10
        val entities: Seq[Entity] = TestingGeometries.polygonsWKT
            .map(g => wktReader.read(g).asInstanceOf[Polygon])
            .zipWithIndex
            .map{case (p, i) => SpatialEntity(i.toString, p)}

        val theta = TileGranularities(entities.map(e => e.env), entities.length, ThetaOption.AVG_x2)
        val index = SpatialIndex(entities.toArray, theta)

        val res: Boolean = entities.forall { e =>
            val p = e.geometry.asInstanceOf[Polygon]
            val fragments: Seq[Polygon] = GridFragmentation.splitPolygon(p, theta)
            if (fragments.length == 1)
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

                val envelopes = fragments.map(p => p.getEnvelope)
                envelopes.forall { e =>
                    val covers = tilesEnvelope.filter(te => te.covers(e))
                    covers.length == 1
                }
            }
        }
        res shouldBe true
    }
}