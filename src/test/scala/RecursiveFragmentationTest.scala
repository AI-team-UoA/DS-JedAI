

import org.locationtech.jts.geom.{Geometry, GeometryFactory, LineString, Polygon}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import utils.decompose.{GeometryUtils, RecursiveFragmentation}

class RecursiveFragmentationTest extends AnyFunSuite {
    val wktReader = new WKTReader()
    val geomFactory = new GeometryFactory()
    val delta = 1e-5
    val lineT = 2e-1
    val polygonT = 5e-2



    /**
     * Test Geometry Collection flattening
     */
    test("GeometryUtils.flattenCollection - GeometryCollections") {
        val polygons: Seq[Geometry] = TestingGeometries.geometryCollectionsWKT.map(g => wktReader.read(g))
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        assert(
            polygons.forall { gc =>
                val geometries: Seq[Geometry] = GeometryUtils.flattenCollection(gc)
                val merged = geometries.foldLeft(emptyGeom)(_ union _)
                val diff = math.abs(merged.getArea - gc.getArea)
                diff < delta
            }
        )
    }

    /**
     * Test simple geometries Split
     */
    test("GeometryUtils.splitPolygon - Polygons") {
        val polygons: Seq[Polygon] = TestingGeometries.polygonsWKT.map(g => wktReader.read(g).asInstanceOf[Polygon])
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        assert(
            polygons.forall { p =>
                val subPolygons: Seq[Polygon] = RecursiveFragmentation.splitPolygon(p, polygonT)
                val merged = subPolygons.foldLeft(emptyGeom)(_ union _)
                val diff = math.abs(merged.getArea - p.getArea)
                diff < delta
            }
        )
    }


    /**
     * Test geometries with Inner rings Split
     */
    test("GeometryUtils.splitPolygon - Ring Polygons") {
        val polygons: Seq[Polygon] = TestingGeometries.ringPolygonsWKT.map(g => wktReader.read(g).asInstanceOf[Polygon])
        val emptyGeom: Geometry = geomFactory.createEmpty(2)
        assert(
            polygons.forall { p =>
                val subPolygons: Seq[Polygon] = RecursiveFragmentation.splitPolygon(p, polygonT)
                val merged = subPolygons.foldLeft(emptyGeom)(_ union _)
                val diff = math.abs(merged.getArea - p.getArea)
                diff < delta
            }
        )
    }


    /**
     * Test lineSting split
     */
    test("GeometryUtils.splitLines") {
        val lineStrings: Seq[LineString] = TestingGeometries.lineStringsWKT.map(g => wktReader.read(g).asInstanceOf[LineString])
        assert(
            lineStrings.forall { l =>
                val lines: Seq[LineString] = RecursiveFragmentation.splitLineString(l, lineT)
                val linesLength: Double = lines.map(_.getLength).sum
                val diff = math.abs(linesLength - l.getLength)
                diff < delta
            }
        )
    }


    /**
     * Test splitBigGeometries
     */
    test("GeometryUtils.splitBigGeometries") {
        val wkt = TestingGeometries.lineStringsWKT ++ TestingGeometries.geometryCollectionsWKT ++
                    TestingGeometries.polygonsWKT ++ TestingGeometries.polygonsWKT
        val geometries: Seq[Geometry] = wkt.map(g => wktReader.read(g))
        assert(
            geometries.forall { g =>
                val res: Seq[Geometry] = RecursiveFragmentation.splitBigGeometries(lineT, polygonT)(g)
                val gArea: Double = res.map(_.getArea).sum
                val diffArea = math.abs(gArea - g.getArea)
                diffArea < delta
            }
        )
    }
}