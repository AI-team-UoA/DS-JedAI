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

package org.locationtech.jts.operation.union;

import java.util.Collection;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

import org.locationtech.jts.operation.union.UnaryUnionOp;
import test.jts.GeometryTestCase;
import test.jts.util.IOUtil;

public class UnaryUnionTest extends GeometryTestCase 
{
	GeometryFactory geomFact = new GeometryFactory();
	
  public UnaryUnionTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    junit.textui.TestRunner.run(UnaryUnionTest.class);
  }

  public void testEmptyCollection()
  throws Exception
  {
    doTest(new String[]{}, "GEOMETRYCOLLECTION EMPTY");
  }

  public void testEmptyPolygon()
  throws Exception
  {
    doTest("POLYGON EMPTY", "POLYGON EMPTY");
  }

  public void testEmptyPointWithLine()
  throws Exception
  {
    doTest(new String[]{ "POINT EMPTY", "LINESTRING (0 0, 1 1)"}, "LINESTRING (0 0, 1 1)");
  }

  public void testPoints()
  throws Exception
  {
    doTest(new String[]{ "POINT (1 1)", "POINT (2 2)"}, "MULTIPOINT ((1 1), (2 2))");
  }

  public void testLineNoding()
  throws Exception
  {
    doTest(new String[]{ "LINESTRING (0 0, 10 0, 5 -5, 5 5)"}, 
        "MULTILINESTRING ((0 0, 5 0), (5 0, 10 0, 5 -5, 5 0), (5 0, 5 5))");
  }

  public void testAll()
  throws Exception
  {
    doTest(new String[]{"GEOMETRYCOLLECTION (POLYGON ((0 0, 0 90, 90 90, 90 0, 0 0)),   POLYGON ((120 0, 120 90, 210 90, 210 0, 120 0)),  LINESTRING (40 50, 40 140),  LINESTRING (160 50, 160 140),  POINT (60 50),  POINT (60 140),  POINT (40 140))"},
    		"GEOMETRYCOLLECTION (POINT (60 140),   LINESTRING (40 90, 40 140), LINESTRING (160 90, 160 140), POLYGON ((0 0, 0 90, 40 90, 90 90, 90 0, 0 0)), POLYGON ((120 0, 120 90, 160 90, 210 90, 210 0, 120 0)))");  }

  private void doTest(String[] inputWKT, String expectedWKT) 
  throws ParseException
  {
    Geometry result;
    Collection geoms = readList(inputWKT);
    if (geoms.size() == 0)
      result = UnaryUnionOp.union(geoms, geomFact);
    else
      result = UnaryUnionOp.union(geoms);
    
    checkEqual(read(expectedWKT), result);
  }

  private void doTest(String inputWKT, String expectedWKT) 
  throws ParseException
  {
    Geometry geom = read(inputWKT);
    Geometry result = UnaryUnionOp.union(geom);
    
    checkEqual(read(expectedWKT), result);
  }

}
