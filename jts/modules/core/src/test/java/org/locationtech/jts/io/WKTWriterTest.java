/*
 * Copyright (c) 2016 Vivid Solutions.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */

package org.locationtech.jts.io;

import org.locationtech.jts.geom.*;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.Ordinate;
import org.locationtech.jts.io.WKTWriter;


/**
 * Test for {@link WKTWriter}.
 *
 * @version 1.7
 */
public class WKTWriterTest extends TestCase {

  PrecisionModel precisionModel = new PrecisionModel(1);
  GeometryFactory geometryFactory = new GeometryFactory(precisionModel, 0);
  WKTWriter writer = new WKTWriter();
  WKTWriter writer3D = new WKTWriter(3);
  WKTWriter writer2DM = new WKTWriter(3);

  public static void main(String args[]) {
    TestRunner.run(suite());
  }

  public WKTWriterTest(String name) { super(name);
    writer2DM.setOutputOrdinates(Ordinate.createXYM());
  }

  public static Test suite() { return new TestSuite(WKTWriterTest.class); }

  public void testProperties() {
    assertEquals(Ordinate.createXY(), writer.getOutputOrdinates());
    assertEquals(Ordinate.createXYZ(), writer3D.getOutputOrdinates());
    assertEquals(Ordinate.createXYM(), writer2DM.getOutputOrdinates());

    GeometryFactory gf = new GeometryFactory(
            PackedCoordinateSequenceFactory.DOUBLE_FACTORY);
    WKTWriter writer3DM = new WKTWriter(4);
    assertEquals(Ordinate.createXYZM(), writer3DM.getOutputOrdinates());

    writer3DM.setOutputOrdinates(Ordinate.createXY());
    assertEquals(Ordinate.createXY(), writer3DM.getOutputOrdinates());
    writer3DM.setOutputOrdinates(Ordinate.createXYZ());
    assertEquals(Ordinate.createXYZ(), writer3DM.getOutputOrdinates());
    writer3DM.setOutputOrdinates(Ordinate.createXYM());
    assertEquals(Ordinate.createXYM(), writer2DM.getOutputOrdinates());
    writer3DM.setOutputOrdinates(Ordinate.createXYZM());
    assertEquals(Ordinate.createXYZM(), writer3DM.getOutputOrdinates());

  }

  public void testWritePoint() {
    Point point = geometryFactory.createPoint(new Coordinate(10, 10));
    assertEquals("POINT (10 10)", writer.write(point).toString());
  }

  public void testWriteLineString() {
    Coordinate[] coordinates = { new Coordinate(10, 10, 0),
                                 new Coordinate(20, 20, 0),
                                 new Coordinate(30, 40, 0) };
    LineString lineString = geometryFactory.createLineString(coordinates);
    assertEquals("LINESTRING (10 10, 20 20, 30 40)", writer.write(lineString).toString());
  }

  public void testWritePolygon() throws Exception {
    Coordinate[] coordinates = { new Coordinate(10, 10, 0),
                                 new Coordinate(10, 20, 0),
                                 new Coordinate(20, 20, 0),
                                 new Coordinate(20, 15, 0),
                                 new Coordinate(10, 10, 0) };
    LinearRing linearRing = geometryFactory.createLinearRing(coordinates);
    Polygon polygon = geometryFactory.createPolygon(linearRing, new LinearRing[] { });
    assertEquals("POLYGON ((10 10, 10 20, 20 20, 20 15, 10 10))", writer.write(polygon).toString());
  }

  public void testWriteMultiPoint() {
    Point[] points = { geometryFactory.createPoint(new Coordinate(10, 10, 0)),
                       geometryFactory.createPoint(new Coordinate(20, 20, 0)) };
    MultiPoint multiPoint = geometryFactory.createMultiPoint(points);
    assertEquals("MULTIPOINT ((10 10), (20 20))", writer.write(multiPoint).toString());
  }

  public void testWriteMultiLineString() {
    Coordinate[] coordinates1 = { new Coordinate(10, 10, 0),
                                  new Coordinate(20, 20, 0) };
    LineString lineString1 = geometryFactory.createLineString(coordinates1);
    Coordinate[] coordinates2 = { new Coordinate(15, 15, 0),
                                  new Coordinate(30, 15, 0) };
    LineString lineString2 = geometryFactory.createLineString(coordinates2);
    LineString[] lineStrings = {lineString1, lineString2};
    MultiLineString multiLineString = geometryFactory.createMultiLineString(lineStrings);
    assertEquals("MULTILINESTRING ((10 10, 20 20), (15 15, 30 15))", writer.write(multiLineString).toString());
  }

  public void testWriteMultiPolygon() throws Exception {
    Coordinate[] coordinates1 = { new Coordinate(10, 10, 0),
                                 new Coordinate(10, 20, 0),
                                 new Coordinate(20, 20, 0),
                                 new Coordinate(20, 15, 0),
                                 new Coordinate(10, 10, 0) };
    LinearRing linearRing1 = geometryFactory.createLinearRing(coordinates1);
    Polygon polygon1 = geometryFactory.createPolygon(linearRing1, new LinearRing[] { });
    Coordinate[] coordinates2 = { new Coordinate(60, 60, 0),
                                 new Coordinate(70, 70, 0),
                                 new Coordinate(80, 60, 0),
                                 new Coordinate(60, 60, 0) };
    LinearRing linearRing2 = geometryFactory.createLinearRing(coordinates2);
    Polygon polygon2 = geometryFactory.createPolygon(linearRing2, new LinearRing[] { });
    Polygon[] polygons = {polygon1, polygon2};
    MultiPolygon multiPolygon = geometryFactory.createMultiPolygon(polygons);
//    System.out.println("MULTIPOLYGON (((10 10, 10 20, 20 20, 20 15, 10 10)), ((60 60, 70 70, 80 60, 60 60)))");
//    System.out.println(writer.write(multiPolygon).toString());
    assertEquals("MULTIPOLYGON (((10 10, 10 20, 20 20, 20 15, 10 10)), ((60 60, 70 70, 80 60, 60 60)))", writer.write(multiPolygon).toString());
  }

  public void testWriteGeometryCollection() {
    Point point1 = geometryFactory.createPoint(new Coordinate(10, 10));
    Point point2 = geometryFactory.createPoint(new Coordinate(30, 30));
    Coordinate[] coordinates = { new Coordinate(15, 15, 0),
                                 new Coordinate(20, 20, 0) };
    LineString lineString1 = geometryFactory.createLineString(coordinates);
    Geometry[] geometries = {point1, point2, lineString1};
    GeometryCollection geometryCollection = geometryFactory.createGeometryCollection(geometries);
    assertEquals("GEOMETRYCOLLECTION (POINT (10 10), POINT (30 30), LINESTRING (15 15, 20 20))", writer.write(geometryCollection).toString());
  }

  public void testWriteLargeNumbers1() {
    PrecisionModel precisionModel = new PrecisionModel(1E9);
    GeometryFactory geometryFactory = new GeometryFactory(precisionModel, 0);
    Point point1 = geometryFactory.createPoint(new Coordinate(123456789012345678d, 10E9));
    assertEquals("POINT (123456789012345680 10000000000)", point1.toText());
  }

  public void testWriteLargeNumbers2() {
    PrecisionModel precisionModel = new PrecisionModel(1E9);
    GeometryFactory geometryFactory = new GeometryFactory(precisionModel, 0);
    Point point1 = geometryFactory.createPoint(new Coordinate(1234d, 10E9));
    assertEquals("POINT (1234 10000000000)", point1.toText());
  }

  public void testWriteLargeNumbers3() {
    PrecisionModel precisionModel = new PrecisionModel(1E9);
    GeometryFactory geometryFactory = new GeometryFactory(precisionModel, 0);
    Point point1 = geometryFactory.createPoint(new Coordinate(123456789012345678000000E9d, 10E9));
    assertEquals("POINT (123456789012345690000000000000000 10000000000)", point1.toText());
  }

  public void testWrite3D() {
    GeometryFactory geometryFactory = new GeometryFactory();
    Point point = geometryFactory.createPoint(new Coordinate(1, 1, 1));
    String wkt = writer3D.write(point);
    assertEquals("POINT Z(1 1 1)", wkt);
    wkt = writer2DM.write(point);
    assertEquals("POINT (1 1)", wkt);
  }

  public void testWrite3D_withNaN() {
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coordinates = { new Coordinate(1, 1),
                                 new Coordinate(2, 2, 2) };
    LineString line = geometryFactory.createLineString(coordinates);
    String wkt = writer3D.write(line);
    assertEquals("LINESTRING Z(1 1 NaN, 2 2 2)", wkt);
    wkt = writer2DM.write(line);
    assertEquals("LINESTRING (1 1, 2 2)", wkt);
  }

}
