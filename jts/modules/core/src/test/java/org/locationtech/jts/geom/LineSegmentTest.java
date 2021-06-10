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
package org.locationtech.jts.geom;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.io.WKTReader;

import junit.framework.TestCase;
import junit.textui.TestRunner;


/**
 * Test named predicate short-circuits
 */
/**
 * @version 1.7
 */
public class LineSegmentTest extends TestCase {

  WKTReader rdr = new WKTReader();

  public static void main(String args[]) {
    TestRunner.run(LineSegmentTest.class);
  }

  public LineSegmentTest(String name) { super(name); }

  private static double ROOT2 = Math.sqrt(2);
  
  public void testProjectionFactor()
  {
    // zero-length line
    LineSegment seg = new LineSegment(10, 0, 10, 0);
    assertTrue(Double.isNaN(seg.projectionFactor(new Coordinate(11, 0))));
    
    LineSegment seg2 = new LineSegment(10, 0, 20, 0);
    assertTrue(seg2.projectionFactor(new Coordinate(11, 0)) == 0.1);
  }
  
  public void testLineIntersection() {
    // simple case
    checkLineIntersection(
        0,0,  10,10,
        0,10, 10,0,
        5,5);

    //Almost collinear - See JTS GitHub issue #464
    checkLineIntersection(
        35613471.6165017, 4257145.306132293, 35613477.7705378, 4257160.528222711,
        35613477.77505724, 4257160.539653536, 35613479.85607389, 4257165.92369170,
        35613477.772841461, 4257160.5339209242 );
  }
  
  private static final double MAX_ABS_ERROR_INTERSECTION = 1e-5;
  
  private void checkLineIntersection(double p1x, double p1y, double p2x, double p2y, 
      double q1x, double q1y, double q2x, double q2y, 
      double expectedx, double expectedy) {
    LineSegment seg1 = new LineSegment(p1x, p1y, p2x, p2y);
    LineSegment seg2 = new LineSegment(q1x, q1y, q2x, q2y);
    
    Coordinate actual = seg1.lineIntersection(seg2);
    Coordinate expected = new Coordinate( expectedx, expectedy );
    double dist = actual.distance(expected);
    //System.out.println("Expected: " + expected + "  Actual: " + actual + "  Dist = " + dist);
    assertTrue(dist <= MAX_ABS_ERROR_INTERSECTION);
  }

  public void testOffset() throws Exception
  {
    checkOffset(0, 0, 10, 10, 0.0, ROOT2, -1, 1);
    checkOffset(0, 0, 10, 10, 0.0, -ROOT2, 1, -1);
    
    checkOffset(0, 0, 10, 10, 1.0, ROOT2, 9, 11);
    checkOffset(0, 0, 10, 10, 0.5, ROOT2, 4, 6);
    
    checkOffset(0, 0, 10, 10, 0.5, -ROOT2, 6, 4);
    checkOffset(0, 0, 10, 10, 0.5, -ROOT2, 6, 4);
    
    checkOffset(0, 0, 10, 10, 2.0, ROOT2, 19, 21);
    checkOffset(0, 0, 10, 10, 2.0, -ROOT2, 21, 19);
    
    checkOffset(0, 0, 10, 10, 2.0, 5 * ROOT2, 15, 25);
    checkOffset(0, 0, 10, 10, -2.0, 5 * ROOT2, -25, -15);

  }

  void checkOffset(double x0, double y0, double x1, double y1, double segFrac, double offset, 
  		double expectedX, double expectedY)
  {
  	LineSegment seg = new LineSegment(x0, y0, x1, y1);
  	Coordinate p = seg.pointAlongOffset(segFrac, offset);
  	
  	assertTrue(equalsTolerance(new Coordinate(expectedX, expectedY), p, 0.000001));
  }
  
  public static boolean equalsTolerance(Coordinate p0, Coordinate p1, double tolerance)
  {
  	if (Math.abs(p0.x - p1.x) > tolerance) return false;
  	if (Math.abs(p0.y - p1.y) > tolerance) return false;
  	return true;
  }
  
  public void testReflect() {
    checkReflect(0, 0, 10, 10, 1,2, 2 ,1 );
    checkReflect(0, 1, 10, 1, 1, 2, 1, 0 );
  }
  
  void checkReflect(double x0, double y0, double x1, double y1, double x, double y, 
      double expectedX, double expectedY)
  {
    LineSegment seg = new LineSegment(x0, y0, x1, y1);
    Coordinate p = seg.reflect(new Coordinate(x, y));
    assertTrue(equalsTolerance(new Coordinate(expectedX, expectedY), p, 0.000001));
  }
  
  public void testOrientationIndexCoordinate()
  {
  	LineSegment seg = new LineSegment(0, 0, 10, 10);
  	checkOrientationIndex(seg, 10, 11, 1);
  	checkOrientationIndex(seg, 10, 9, -1);
  	
  	checkOrientationIndex(seg, 11, 11, 0);
  	
  	checkOrientationIndex(seg, 11, 11.0000001, 1);
  	checkOrientationIndex(seg, 11, 10.9999999, -1);
  	
  	checkOrientationIndex(seg, -2, -1.9999999, 1);
  	checkOrientationIndex(seg, -2, -2.0000001, -1);
  }
  
  public void testOrientationIndexSegment()
  {
  	LineSegment seg = new LineSegment(100, 100, 110, 110);
  	
  	checkOrientationIndex(seg, 100, 101, 105, 106, 1);
  	checkOrientationIndex(seg, 100, 99, 105, 96, -1);
  	
  	checkOrientationIndex(seg, 200, 200, 210, 210, 0);
  	
  }
  
  void checkOrientationIndex(double x0, double y0, double x1, double y1, double px, double py, 
  		int expectedOrient)
  {
  	LineSegment seg = new LineSegment(x0, y0, x1, y1);
  	checkOrientationIndex(seg, px, py, expectedOrient);
  }
  
  void checkOrientationIndex(LineSegment seg, 
  		double px, double py, 
  		int expectedOrient)
  {
  	Coordinate p = new Coordinate(px, py);
  	int orient = seg.orientationIndex(p);
  	assertTrue(orient == expectedOrient);
  }
  
  void checkOrientationIndex(LineSegment seg, 
  		double s0x, double s0y, 
  		double s1x, double s1y, 
  		int expectedOrient)
  {
  	LineSegment seg2 = new LineSegment(s0x, s0y, s1x, s1y);
  	int orient = seg.orientationIndex(seg2);
  	assertTrue(orient == expectedOrient);
  }
  

}
