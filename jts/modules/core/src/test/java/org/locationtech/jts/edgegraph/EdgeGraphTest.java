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

package org.locationtech.jts.edgegraph;

import java.util.List;

import org.locationtech.jts.edgegraph.EdgeGraph;
import org.locationtech.jts.edgegraph.EdgeGraphBuilder;
import org.locationtech.jts.edgegraph.HalfEdge;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.ParseException;

import junit.framework.TestCase;
import junit.textui.TestRunner;
import test.jts.util.IOUtil;


public class EdgeGraphTest extends TestCase {
  public static void main(String args[]) {
    TestRunner.run(EdgeGraphTest.class);
  }


  public EdgeGraphTest(String name) { super(name); }

  public void testNode() throws Exception
  {
    EdgeGraph graph = build("MULTILINESTRING((0 0, 1 0), (0 0, 0 1), (0 0, -1 0))");
    checkEdgeRing(graph, new Coordinate(0, 0), 
        new Coordinate[] { new Coordinate(1, 0),
      new Coordinate(0, 1), new Coordinate(-1, 0)
        });
    checkNodeValid(graph, new Coordinate(0, 0), new Coordinate(1, 0));
    checkEdge(graph, new Coordinate(0, 0), new Coordinate(1, 0));
  }

  /**
   * This test produced an error using the original buggy sorting algorithm
   * (in {@link HalfEdge#insert(HalfEdge)}).
   */
  public void testCCWAfterInserts() {
    EdgeGraph graph = new EdgeGraph();
    HalfEdge e1 = addEdge(graph, 50, 39, 35, 42);
    addEdge(graph, 50, 39, 50, 60);
    addEdge(graph, 50, 39, 68, 35);
    checkNodeValid(e1);
  }

  public void testCCWAfterInserts2() {
    EdgeGraph graph = new EdgeGraph();
    HalfEdge e1 = addEdge(graph, 50, 200, 0, 200);
    addEdge(graph, 50, 200, 190, 50);
    addEdge(graph, 50, 200, 200, 200);
    checkNodeValid(e1);
  }


  private void checkEdgeRing(EdgeGraph graph, Coordinate p,
      Coordinate[] dest) {
    HalfEdge e = graph.findEdge(p, dest[0]);
    HalfEdge onext = e;
    int i = 0;
    do {
      assertTrue(onext.dest().equals2D(dest[i++]));
      onext = onext.oNext();
    } while (onext != e);
   
  }

  private void checkEdge(EdgeGraph graph, Coordinate p0, Coordinate p1) {
    HalfEdge e = graph.findEdge(p0, p1);
    assertNotNull(e);
  }

  private void checkNodeValid(EdgeGraph graph, Coordinate p0, Coordinate p1) {
    HalfEdge e = graph.findEdge(p0, p1);
    boolean isNodeValid = e.isEdgesSorted();
    assertTrue("Found non-sorted edges around node " + e, isNodeValid); 
  }


  private void checkNodeValid(HalfEdge e) {
    boolean isNodeValid = e.isEdgesSorted();
    assertTrue("Found non-sorted edges around node " + e, isNodeValid); 
  }
  
  private EdgeGraph build(String wkt) throws ParseException {
    return build(new String[] { wkt });
  }

  private EdgeGraph build(String[] wkt) throws ParseException {
    List geoms = IOUtil.readWKT(wkt);
    return EdgeGraphBuilder.build(geoms);
  }

  private HalfEdge addEdge(EdgeGraph graph, double p0x, double p0y, double p1x, double p1y) {
    return graph.addEdge(new Coordinate(p0x, p0y), new Coordinate(p1x, p1y));
  }

}
