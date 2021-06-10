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

package org.locationtech.jts.dissolve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.locationtech.jts.edgegraph.HalfEdge;
import org.locationtech.jts.edgegraph.MarkHalfEdge;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.*;


/**
 * Dissolves the linear components 
 * from a collection of {@link Geometry}s
 * into a set of maximal-length {@link Linestring}s
 * in which every unique segment appears once only.
 * The output linestrings run between node vertices
 * of the input, which are vertices which have
 * either degree 1, or degree 3 or greater.
 * <p>
 * Use cases for dissolving linear components
 * include generalization 
 * (in particular, simplifying polygonal coverages), 
 * and visualization 
 * (in particular, avoiding symbology conflicts when
 * depicting shared polygon boundaries).
 * <p>
 * This class does <b>not</b> node the input lines.
 * If there are line segments crossing in the input, 
 * they will still cross in the output.
 * 
 * @author Martin Davis
 *
 */
public class LineDissolver 
{
  /**
   * Dissolves the linear components in a geometry.
   * 
   * @param g the geometry to dissolve
   * @return the dissolved lines
   */
  public static Geometry dissolve(Geometry g)
  {
    LineDissolver d = new LineDissolver();
    d.add(g);
    return d.getResult();
  }
  
  private Geometry result;
  private GeometryFactory factory;
  private DissolveEdgeGraph graph;
  private List lines = new ArrayList();

  public LineDissolver()
  {
    graph = new DissolveEdgeGraph();
  }
  
  /**
   * Adds a {@link Geometry} to be dissolved. 
   * Any number of geometries may be added by calling this method multiple times.
   * Any type of Geometry may be added.  The constituent linework will be
   * extracted to be dissolved.
   * 
   * @param geometry geometry to be line-merged
   */  
  public void add(Geometry geometry) {
    geometry.apply(new GeometryComponentFilter() {
      public void filter(Geometry component) {
        if (component instanceof LineString) {
          add((LineString)component);
        }
      }      
    });
  }
  /**
   * Adds a collection of Geometries to be processed. May be called multiple times.
   * Any dimension of Geometry may be added; the constituent linework will be
   * extracted.
   * 
   * @param geometries the geometries to be line-merged
   */
  public void add(Collection geometries) 
  {
    for (Iterator i = geometries.iterator(); i.hasNext(); ) {
      Geometry geometry = (Geometry) i.next();
      add(geometry);
    }
  }
  
  private void add(LineString lineString) {
    if (factory == null) {
      this.factory = lineString.getFactory();
    }
    CoordinateSequence seq = lineString.getCoordinateSequence();
    boolean doneStart = false;
    for (int i = 1; i < seq.size(); i++) {
      DissolveHalfEdge e = (DissolveHalfEdge) graph.addEdge(seq.getCoordinate(i-1), seq.getCoordinate(i));
      // skip zero-length edges
      if (e == null) continue;
      /**
       * Record source initial segments, so that they can be reflected in output when needed
       * (i.e. during formation of isolated rings)
       */
      if (! doneStart) {
        e.setStart();
        doneStart = true;
      }
    }
  }
  
  /**
   * Gets the dissolved result as a MultiLineString.
   * 
   * @return the dissolved lines
   */
  public Geometry getResult()
  {
    if (result == null)
      computeResult();
    return result;
  }

  private void computeResult() {
    Collection edges = graph.getVertexEdges();
    for (Iterator i = edges.iterator(); i.hasNext(); ) {
      HalfEdge e = (HalfEdge) i.next();
      if (MarkHalfEdge.isMarked(e)) continue;
      process(e);
    }
    result = factory.buildGeometry(lines);
  }

  private Stack nodeEdgeStack = new Stack();
  
  private void process(HalfEdge e) {
    HalfEdge eNode = e.prevNode();
    // if edge is in a ring, just process this edge
    if (eNode == null)
      eNode = e;
    stackEdges(eNode);
    // extract lines from node edges in stack
    buildLines();
  }

  /**
   * For each edge in stack
   * (which must originate at a node)
   * extracts the line it initiates.
   */
  private void buildLines() {
    while (! nodeEdgeStack.empty()) {
      HalfEdge e = (HalfEdge) nodeEdgeStack.pop();
      if (MarkHalfEdge.isMarked(e))
        continue;
      buildLine(e);
    }
  }
  
  private DissolveHalfEdge ringStartEdge;
  
  /**
   * Updates the tracked ringStartEdge
   * if the given edge has a lower origin
   * (using the standard {@link Coordinate} ordering).
   * 
   * Identifying the lowest starting node meets two goals:
   * <ul>
   * <li>It ensures that isolated input rings are created using the original node and orientation
   * <li>For isolated rings formed from multiple input linestrings, 
   * it provides a canonical node and orientation for the output
   * (rather than essentially random, and thus hard to test).
   * </ul>
   * 
   * @param e
   */
  private void updateRingStartEdge(DissolveHalfEdge e)
  {
    if (! e.isStart()) {
      e = (DissolveHalfEdge) e.sym();
      if (! e.isStart()) return;
    }
    // here e is known to be a start edge
    if (ringStartEdge == null) {
      ringStartEdge = e;
      return;
    }
    if (e.orig().compareTo(ringStartEdge.orig()) < 0) {
      ringStartEdge = e;
    }
  }
  
  /**
   * Builds a line starting from the given edge.
   * The start edge origin is a node (valence = 1 or >= 3), 
   * unless it is part of a pure ring.
   * A pure ring has no other incident lines.
   * In this case the start edge may occur anywhere on the ring.
   * 
   * The line is built up to the next node encountered,
   * or until the start edge is re-encountered
   * (which happens if the edges form a ring).
   * 
   * @param eStart
   */
  private void buildLine(HalfEdge eStart) {
    CoordinateList line = new CoordinateList();
    DissolveHalfEdge e = (DissolveHalfEdge) eStart;
    ringStartEdge = null;
    
    MarkHalfEdge.markBoth(e);
    line.add(e.orig().copy(), false);
    // scan along the path until a node is found (if one exists)
    while (e.sym().degree() == 2) {
      updateRingStartEdge(e);
      DissolveHalfEdge eNext = (DissolveHalfEdge) e.next();
      // check if edges form a ring - if so, we're done
      if (eNext == eStart)  {
        buildRing(ringStartEdge);
        return;
      }
      // add point to line, and move to next edge
      line.add(eNext.orig().copy(), false);
      e = eNext;
      MarkHalfEdge.markBoth(e);
    }
    // add final node
    line.add(e.dest().clone(), false);
    
    // queue up the final node edges
    stackEdges(e.sym());
    // store the scanned line
    addLine(line);
  }

  private void buildRing(HalfEdge eStartRing) {
    CoordinateList line = new CoordinateList();
    HalfEdge e = eStartRing;
    
    line.add(e.orig().copy(), false);
    // scan along the path until a node is found (if one exists)
    while (e.sym().degree() == 2) {
      HalfEdge eNext = e.next();
      // check if edges form a ring - if so, we're done
      if (eNext == eStartRing)
        break;
      
      // add point to line, and move to next edge
      line.add(eNext.orig().copy(), false);
      e = eNext;
    }
    // add final node
    line.add(e.dest().copy(), false);
    
    // store the scanned line
    addLine(line);
  }

  private void addLine(CoordinateList line) {
    lines.add(factory.createLineString(line.toCoordinateArray()));
  }

  /**
   * Adds edges around this node to the stack.
   * 
   * @param node
   */
  private void stackEdges(HalfEdge node) {
    HalfEdge e = node;
    do {
      if (! MarkHalfEdge.isMarked(e))
        nodeEdgeStack.add(e);
      e = e.oNext();
    } while (e != node);

  }

}
