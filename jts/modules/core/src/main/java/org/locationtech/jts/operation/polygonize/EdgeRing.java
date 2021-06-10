
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


package org.locationtech.jts.operation.polygonize;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.algorithm.locate.PointOnGeometryLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateArrays;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.planargraph.DirectedEdge;
import org.locationtech.jts.util.Assert;
import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.algorithm.locate.PointOnGeometryLocator;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.util.Assert;


/**
 * Represents a ring of {@link PolygonizeDirectedEdge}s which form
 * a ring of a polygon.  The ring may be either an outer shell or a hole.
 *
 * @version 1.7
 */
class EdgeRing {

  /**
   * Find the innermost enclosing shell EdgeRing containing the argument EdgeRing, if any.
   * The innermost enclosing ring is the <i>smallest</i> enclosing ring.
   * The algorithm used depends on the fact that:
   * <br>
   *  ring A contains ring B iff envelope(ring A) contains envelope(ring B)
   * <br>
   * This routine is only safe to use if the chosen point of the hole
   * is known to be properly contained in a shell
   * (which is guaranteed to be the case if the hole does not touch its shell)
   * <p>
   * To improve performance of this function the caller should 
   * make the passed shellList as small as possible (e.g.
   * by using a spatial index filter beforehand).
   * 
   * @return containing EdgeRing, if there is one
   * or null if no containing EdgeRing is found
   */
  public static EdgeRing findEdgeRingContaining(EdgeRing testEr, List erList)
  {
    LinearRing testRing = testEr.getRing();
    Envelope testEnv = testRing.getEnvelopeInternal();
    Coordinate testPt = testRing.getCoordinateN(0);

    EdgeRing minRing = null;
    Envelope minRingEnv = null;
    for (Iterator it = erList.iterator(); it.hasNext(); ) {
      EdgeRing tryEdgeRing = (EdgeRing) it.next();
      LinearRing tryRing = tryEdgeRing.getRing();
      Envelope tryShellEnv = tryRing.getEnvelopeInternal();
      // the hole envelope cannot equal the shell envelope
      // (also guards against testing rings against themselves)
      if (tryShellEnv.equals(testEnv)) continue;
      
      // hole must be contained in shell
      if (! tryShellEnv.contains(testEnv)) continue;
      
      testPt = CoordinateArrays.ptNotInList(testRing.getCoordinates(), tryEdgeRing.getCoordinates());
 
      /**
       * If testPt is null it indicates that the hole is exactly surrounded by the tryShell.
       * This should not happen for fully noded/dissolved linework.
       * For now just ignore this hole and continue - this should produce
       * "best effort" output.
       * In futher could flag this as an error (invalid ring).
       */
      if (testPt == null) continue;
      
      boolean isContained =  tryEdgeRing.isInRing(testPt);

      // check if the new containing ring is smaller than the current minimum ring
      if (isContained) {
        if (minRing == null
            || minRingEnv.contains(tryShellEnv)) {
          minRing = tryEdgeRing;
          minRingEnv = minRing.getRing().getEnvelopeInternal();
        }
      }
    }
    return minRing;
  }
  
  /**
   * Traverses a ring of DirectedEdges, accumulating them into a list.
   * This assumes that all dangling directed edges have been removed
   * from the graph, so that there is always a next dirEdge.
   *
   * @param startDE the DirectedEdge to start traversing at
   * @return a List of DirectedEdges that form a ring
   */
  public static List findDirEdgesInRing(PolygonizeDirectedEdge startDE)
  {
    PolygonizeDirectedEdge de = startDE;
    List edges = new ArrayList();
    do {
      edges.add(de);
      de = de.getNext();
      Assert.isTrue(de != null, "found null DE in ring");
      Assert.isTrue(de == startDE || ! de.isInRing(), "found DE already in ring");
    } while (de != startDE);
    return edges;
  }
  
  private GeometryFactory factory;

  private List deList = new ArrayList();
  private DirectedEdge lowestEdge = null;
  
  // cache the following data for efficiency
  private LinearRing ring = null;
  private IndexedPointInAreaLocator locator;
  
  private Coordinate[] ringPts = null;
  private List holes;
  private EdgeRing shell;
  private boolean isHole;
  private boolean isProcessed = false;
  private boolean isIncludedSet = false;
  private boolean isIncluded = false;

  public EdgeRing(GeometryFactory factory)
  {
    this.factory = factory;
  }

  public void build(PolygonizeDirectedEdge startDE) {
    PolygonizeDirectedEdge de = startDE;
    do {
      add(de);
      de.setRing(this);
      de = de.getNext();
      Assert.isTrue(de != null, "found null DE in ring");
      Assert.isTrue(de == startDE || ! de.isInRing(), "found DE already in ring");
    } while (de != startDE);
  }
  
  /**
   * Adds a {@link DirectedEdge} which is known to form part of this ring.
   * @param de the {@link DirectedEdge} to add.
   */
  private void add(DirectedEdge de)
  {
    deList.add(de);
  }

  /**
   * Tests whether this ring is a hole.
   * @return <code>true</code> if this ring is a hole
   */
  public boolean isHole()
  {
    return isHole;
  }
  
  /**
   * Computes whether this ring is a hole.
   * Due to the way the edges in the polygonization graph are linked,
   * a ring is a hole if it is oriented counter-clockwise.
   */
  public void computeHole()
  {
    LinearRing ring = getRing();
    isHole = Orientation.isCCW(ring.getCoordinates());
  }

  /**
   * Adds a hole to the polygon formed by this ring.
   * @param hole the {@link LinearRing} forming the hole.
   */
  public void addHole(LinearRing hole) {
    if (holes == null)
      holes = new ArrayList();
    holes.add(hole);
  }

  /**
   * Adds a hole to the polygon formed by this ring.
   * @param hole the {@link LinearRing} forming the hole.
   */
  public void addHole(EdgeRing holeER) {
    holeER.setShell(this);
    LinearRing hole = holeER.getRing();
    if (holes == null)
      holes = new ArrayList();
    holes.add(hole);
  }

  /**
   * Computes the {@link Polygon} formed by this ring and any contained holes.
   *
   * @return the {@link Polygon} formed by this ring and its holes.
   */
  public Polygon getPolygon()
  {
    LinearRing[] holeLR = null;
    if (holes != null) {
      holeLR = new LinearRing[holes.size()];
      for (int i = 0; i < holes.size(); i++) {
        holeLR[i] = (LinearRing) holes.get(i);
      }
    }
    Polygon poly = factory.createPolygon(ring, holeLR);
    return poly;
  }

  /**
   * Tests if the {@link LinearRing} ring formed by this edge ring is topologically valid.
   * 
   * @return true if the ring is valid
   */
  public boolean isValid()
  {
    getCoordinates();
    if (ringPts.length <= 3) return false;
    getRing();
    return ring.isValid();
  }

  public boolean isIncludedSet() {
    return isIncludedSet;
  }

  public boolean isIncluded() {
    return isIncluded;
  }

  public void setIncluded(boolean isIncluded) {
    this.isIncluded = isIncluded;
    this.isIncludedSet = true;
  }

  private PointOnGeometryLocator getLocator() {
    if (locator == null) {
      locator = new IndexedPointInAreaLocator(getRing());
    }
    return locator;
  }
  
  public boolean isInRing(Coordinate pt) {
    /**
     * Use an indexed point-in-polygon for performance
     */
    return Location.EXTERIOR != getLocator().locate(pt);
    //return PointLocation.isInRing(pt, getCoordinates());
  }
  
  /**
   * Computes the list of coordinates which are contained in this ring.
   * The coordinates are computed once only and cached.
   *
   * @return an array of the {@link Coordinate}s in this ring
   */
  private Coordinate[] getCoordinates()
  {
    if (ringPts == null) {
      CoordinateList coordList = new CoordinateList();
      for (Iterator i = deList.iterator(); i.hasNext(); ) {
        DirectedEdge de = (DirectedEdge) i.next();
        PolygonizeEdge edge = (PolygonizeEdge) de.getEdge();
        addEdge(edge.getLine().getCoordinates(), de.getEdgeDirection(), coordList);
      }
      ringPts = coordList.toCoordinateArray();
    }
    return ringPts;
  }

  /**
   * Gets the coordinates for this ring as a {@link LineString}.
   * Used to return the coordinates in this ring
   * as a valid geometry, when it has been detected that the ring is topologically
   * invalid.
   * @return a {@link LineString} containing the coordinates in this ring
   */
  public LineString getLineString()
  {
    getCoordinates();
    return factory.createLineString(ringPts);
  }

  /**
   * Returns this ring as a {@link LinearRing}, or null if an Exception occurs while
   * creating it (such as a topology problem). Details of problems are written to
   * standard output.
   */
  public LinearRing getRing()
  {
    if (ring != null) return ring;
    getCoordinates();
    if (ringPts.length < 3) System.out.println(ringPts);
    try {
      ring = factory.createLinearRing(ringPts);
    }
    catch (Exception ex) {
      System.out.println(ringPts);
    }
    return ring;
  }

  private static void addEdge(Coordinate[] coords, boolean isForward, CoordinateList coordList)
  {
    if (isForward) {
      for (int i = 0; i < coords.length; i++) {
        coordList.add(coords[i], false);
      }
    }
    else {
      for (int i = coords.length - 1; i >= 0; i--) {
        coordList.add(coords[i], false);
      }
    }
  }

  /**
   * Sets the containing shell ring of a ring that has been determined to be a hole.
   * 
   * @param shell the shell ring
   */
  public void setShell(EdgeRing shell) {
    this.shell = shell;
  }
  
  /**
   * Tests whether this ring has a shell assigned to it.
   * 
   * @return true if the ring has a shell
   */
  public boolean hasShell() {
    return shell != null;
  }
  
  /**
   * Gets the shell for this ring.  The shell is the ring itself if it is not a hole, otherwise its parent shell.
   * 
   * @return the shell for this ring
   */
  public EdgeRing getShell() {
    if (isHole()) return shell;
    return this;
  }
  /**
   * Tests whether this ring is an outer hole.
   * A hole is an outer hole if it is not contained by a shell.
   * 
   * @return true if the ring is an outer hole.
   */
  public boolean isOuterHole() {
    if (! isHole) return false;
    return ! hasShell();
  }
  
  /**
   * Tests whether this ring is an outer shell.
   * 
   * @return true if the ring is an outer shell.
   */
  public boolean isOuterShell() {
    return getOuterHole() != null;
  }
  
  /**
   * Gets the outer hole of a shell, if it has one.
   * An outer hole is one that is not contained
   * in any other shell.  
   * Each disjoint connected group of shells
   * is surrounded by an outer hole.
   * 
   * @return the outer hole edge ring, or null
   */
  public EdgeRing getOuterHole()
  {
    /*
     * Only shells can have outer holes
     */
    if (isHole()) return null;
    /*
     * A shell is an outer shell if any edge is also in an outer hole.
     * A hole is an outer hole if it is not contained by a shell.
     */
    for (int i = 0; i < deList.size(); i++) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) deList.get(i);
      EdgeRing adjRing = ((PolygonizeDirectedEdge) de.getSym()).getRing();
      if (adjRing.isOuterHole()) return adjRing;
    }
    return null;    
  }

  /**
   * Updates the included status for currently non-included shells
   * based on whether they are adjacent to an included shell.
   */
  public void updateIncluded() {
    if (isHole()) return;
    for (int i = 0; i < deList.size(); i++) {
      PolygonizeDirectedEdge de = (PolygonizeDirectedEdge) deList.get(i);
      EdgeRing adjShell = ((PolygonizeDirectedEdge) de.getSym()).getRing().getShell();
      
      if (adjShell != null && adjShell.isIncludedSet()) {
        // adjacent ring has been processed, so set included to inverse of adjacent included
        setIncluded(! adjShell.isIncluded());
        return;
      }
    }
  }

  /**
   * Gets a string representation of this object.
   * 
   * @return a string representing the object 
   */
  public String toString() {
    return WKTWriter.toLineString(new CoordinateArraySequence(getCoordinates()));
  }
  
  /**
   * @return whether the ring has been processed
   */
  public boolean isProcessed() {
    return isProcessed;
  }

  /**
   * @param isProcessed whether the ring has been processed
   */
  public void setProcessed(boolean isProcessed) {
    this.isProcessed = isProcessed;
  }

  /**
   * Compares EdgeRings based on their envelope,
   * using the standard lexicographic ordering.
   * This ordering is sufficient to make edge ring sorting deterministic.
   * 
   * @author mbdavis
   *
   */
  static class EnvelopeComparator implements Comparator {
    public int compare(Object obj0, Object obj1) {
      EdgeRing r0 = (EdgeRing) obj0;
      EdgeRing r1 = (EdgeRing) obj1;
      return r0.getRing().getEnvelope().compareTo(r1.getRing().getEnvelope());
    }
    
  }

}
