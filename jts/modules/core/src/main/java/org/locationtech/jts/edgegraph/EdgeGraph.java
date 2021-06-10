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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Coordinate;


/**
 * A graph comprised of {@link HalfEdge}s.
 * It supports tracking the vertices in the graph
 * via edges incident on them, 
 * to allow efficient lookup of edges and vertices.
 * <p>
 * This class may be subclassed to use a 
 * different subclass of HalfEdge,
 * by overriding {@link #createEdge(Coordinate)}.
 * If additional logic is required to initialize
 * edges then {@link EdgeGraph#addEdge(Coordinate, Coordinate)}
 * can be overridden as well.
 * 
 * @author Martin Davis
 *
 */
public class EdgeGraph 
{
  private Map vertexMap = new HashMap();
  
  public EdgeGraph() {
  }

  /**
   * Creates a single HalfEdge.
   * Override to use a different HalfEdge subclass.
   * 
   * @param orig the origin location
   * @return a new HalfEdge with the given origin
   */
  protected HalfEdge createEdge(Coordinate orig)
  {
    return new HalfEdge(orig);
  }

  private HalfEdge create(Coordinate p0, Coordinate p1)
  {
    HalfEdge e0 = createEdge(p0);
    HalfEdge e1 = createEdge(p1);
    e0.link(e1);
    return e0;
  }
  
  /**
   * Adds an edge between the coordinates orig and dest
   * to this graph.
   * Only valid edges can be added (in particular, zero-length segments cannot be added)
   * 
   * @param orig the edge origin location
   * @param dest the edge destination location.
   * @return the created edge
   * @return null if the edge was invalid and not added
   * 
   * @see #isValidEdge(Coordinate, Coordinate)
   */
  public HalfEdge addEdge(Coordinate orig, Coordinate dest) {
    if (! isValidEdge(orig, dest)) return null;
    
    /**
     * Attempt to find the edge already in the graph.
     * Return it if found.
     * Otherwise, use a found edge with same origin (if any) to construct new edge. 
     */
    HalfEdge eAdj = (HalfEdge) vertexMap.get(orig);
    HalfEdge eSame = null;
    if (eAdj != null) {
      eSame = eAdj.find(dest);
    }
    if (eSame != null) {
      return eSame;
    }
    
    HalfEdge e = insert(orig, dest, eAdj);
    return e;
  }

  /**
   * Tests if the given coordinates form a valid edge (with non-zero length).
   * 
   * @param orig the start coordinate
   * @param dest the end coordinate
   * @return true if the edge formed is valid
   */
  public static boolean isValidEdge(Coordinate orig, Coordinate dest) {
    int cmp = dest.compareTo(orig);
    return cmp != 0;
  }

  /**
   * Inserts an edge not already present into the graph.
   * 
   * @param orig the edge origin location
   * @param dest the edge destination location
   * @param eAdj an existing edge with same orig (if any)
   * @return the created edge
   */
  private HalfEdge insert(Coordinate orig, Coordinate dest, HalfEdge eAdj) {
    // edge does not exist, so create it and insert in graph
    HalfEdge e = create(orig, dest);
    if (eAdj != null) {
      eAdj.insert(e);
    }
    else {
      // add halfedges to to map
      vertexMap.put(orig, e);
    }
    
    HalfEdge eAdjDest = (HalfEdge) vertexMap.get(dest);
    if (eAdjDest != null) {
      eAdjDest.insert(e.sym());
    }
    else {
      vertexMap.put(dest, e.sym());
    }
    return e;
  }

  public Collection getVertexEdges()
  {
    return vertexMap.values();
  }

  /**
   * Finds an edge in this graph with the given origin
   * and destination, if one exists.
   * 
   * @param orig the origin location
   * @param dest the destination location.
   * @return an edge with the given orig and dest, or null if none exists
   */
  public HalfEdge findEdge(Coordinate orig, Coordinate dest) {
    HalfEdge e = (HalfEdge) vertexMap.get(orig);
    if (e == null) return null;
    return e.find(dest);
  }
}
