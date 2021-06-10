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

package org.locationtech.jts.simplify;

import java.util.HashMap;
import java.util.Map;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * Simplifies a geometry and ensures that
 * the result is a valid geometry having the
 * same dimension and number of components as the input,
 * and with the components having the same topological 
 * relationship.
 * <p>
 * If the input is a polygonal geometry
 * ( {@link Polygon} or {@link MultiPolygon} ):
 * <ul>
 * <li>The result has the same number of shells and holes as the input,
 * with the same topological structure
 * <li>The result rings touch at <b>no more</b> than the number of touching points in the input
 * (although they may touch at fewer points).  
 * The key implication of this statement is that if the 
 * input is topologically valid, so is the simplified output. 
 * </ul>
 * For linear geometries, if the input does not contain
 * any intersecting line segments, this property
 * will be preserved in the output.
 * <p>
 * For all geometry types, the result will contain 
 * enough vertices to ensure validity.  For polygons
 * and closed linear geometries, the result will have at
 * least 4 vertices; for open linestrings the result
 * will have at least 2 vertices.
 * <p>
 * All geometry types are handled. 
 * Empty and point geometries are returned unchanged.
 * Empty geometry components are deleted.
 * <p>
 * The simplification uses a maximum-distance difference algorithm
 * similar to the Douglas-Peucker algorithm.
 *
 * <h3>KNOWN BUGS</h3>
 * <ul>
 * <li>May create invalid topology if there are components which are 
 * small relative to the tolerance value.
 * In particular, if a small hole is very near an edge, it is possible for the edge to be moved by
 * a relatively large tolerance value and end up with the hole outside the result shell
 * (or inside another hole).
 * Similarly, it is possible for a small polygon component to end up inside
 * a nearby larger polygon.
 * A workaround is to test for this situation in post-processing and remove
 * any invalid holes or polygons.
 * </ul>
 * 
 * @author Martin Davis
 * @see DouglasPeuckerSimplifier
 *
 */
public class TopologyPreservingSimplifier
{
  public static Geometry simplify(Geometry geom, double distanceTolerance)
  {
    TopologyPreservingSimplifier tss = new TopologyPreservingSimplifier(geom);
    tss.setDistanceTolerance(distanceTolerance);
    return tss.getResultGeometry();
  }

  private Geometry inputGeom;
  private TaggedLinesSimplifier lineSimplifier = new TaggedLinesSimplifier();
  private Map linestringMap;

  public TopologyPreservingSimplifier(Geometry inputGeom)
  {
    this.inputGeom = inputGeom;
 }

  /**
   * Sets the distance tolerance for the simplification.
   * All vertices in the simplified geometry will be within this
   * distance of the original geometry.
   * The tolerance value must be non-negative.  A tolerance value
   * of zero is effectively a no-op.
   *
   * @param distanceTolerance the approximation tolerance to use
   */
  public void setDistanceTolerance(double distanceTolerance) {
    if (distanceTolerance < 0.0)
      throw new IllegalArgumentException("Tolerance must be non-negative");
    lineSimplifier.setDistanceTolerance(distanceTolerance);
  }

  public Geometry getResultGeometry() 
  {
    // empty input produces an empty result
    if (inputGeom.isEmpty()) return inputGeom.copy();
    
    linestringMap = new HashMap();
    inputGeom.apply(new LineStringMapBuilderFilter(this));
    lineSimplifier.simplify(linestringMap.values());
    Geometry result = (new LineStringTransformer(linestringMap)).transform(inputGeom);
    return result;
  }

  static class LineStringTransformer
      extends GeometryTransformer
  {
    private Map linestringMap;
    
    public LineStringTransformer(Map linestringMap) {
      this.linestringMap = linestringMap;
    }
    
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent)
    {
      if (coords.size() == 0) return null;
    	// for linear components (including rings), simplify the linestring
      if (parent instanceof LineString) {
        TaggedLineString taggedLine = (TaggedLineString) linestringMap.get(parent);
        return createCoordinateSequence(taggedLine.getResultCoordinates());
      }
      // for anything else (e.g. points) just copy the coordinates
      return super.transformCoordinates(coords, parent);
    }
  }

  /**
   * A filter to add linear geometries to the linestring map 
   * with the appropriate minimum size constraint.
   * Closed {@link LineString}s (including {@link LinearRing}s
   * have a minimum output size constraint of 4, 
   * to ensure the output is valid.
   * For all other linestrings, the minimum size is 2 points.
   * 
   * @author Martin Davis
   *
   */
  static class LineStringMapBuilderFilter
      implements GeometryComponentFilter
  {
    TopologyPreservingSimplifier tps;
    
    LineStringMapBuilderFilter(TopologyPreservingSimplifier tps) {
      this.tps = tps;
    }
    
    /**
     * Filters linear geometries.
     * 
     * geom a geometry of any type 
     */
    public void filter(Geometry geom)
    {
      if (geom instanceof LineString) {
        LineString line = (LineString) geom;
        // skip empty geometries
        if (line.isEmpty()) return;
        
        int minSize = ((LineString) line).isClosed() ? 4 : 2;
        TaggedLineString taggedLine = new TaggedLineString((LineString) line, minSize);
        tps.linestringMap.put(line, taggedLine);
      }
    }
  }

}

