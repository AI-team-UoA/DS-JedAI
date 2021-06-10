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
package org.locationtech.jts.algorithm.locate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.locationtech.jts.algorithm.RayCrossingCounter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.util.LinearComponentExtracter;
import org.locationtech.jts.index.ArrayListVisitor;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.intervalrtree.SortedPackedIntervalRTree;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.LinearComponentExtracter;
import org.locationtech.jts.index.intervalrtree.SortedPackedIntervalRTree;


/**
 * Determines the {@link Location} of {@link Coordinate}s relative to
 * an areal geometry, using indexing for efficiency.
 * This algorithm is suitable for use in cases where
 * many points will be tested against a given area.
 * <p>
 * The Location is computed precisely, in that points
 * located on the geometry boundary or segments will 
 * return {@link Location.BOUNDARY}.
 * <p>
 * {@link Polygonal} and {@link LinearRing} geometries
 * are supported.
 * <p>
 * The index is lazy-loaded, which allows
 * creating instances even if they are not used.
 * <p>
 * Thread-safe and immutable.
 *
 * @author Martin Davis
 *
 */
public class IndexedPointInAreaLocator 
  implements PointOnGeometryLocator
{
  
  private Geometry geom;
  private IntervalIndexedGeometry index = null;
  
  /**
   * Creates a new locator for a given {@link Geometry}.
   * {@link Polygonal} and {@link LinearRing} geometries
   * are supported.
   * 
   * @param g the Geometry to locate in
   */
  public IndexedPointInAreaLocator(Geometry g)
  {
    if (! (g instanceof Polygonal  || g instanceof LinearRing))
      throw new IllegalArgumentException("Argument must be Polygonal or LinearRing");
    geom = g;
  }
    
  /**
   * Determines the {@link Location} of a point in an areal {@link Geometry}.
   * 
   * @param p the point to test
   * @return the location of the point in the geometry  
   */
  public int locate(Coordinate p)
  {
    // avoid calling synchronized method improves performance
    if (index == null) createIndex();
    
    RayCrossingCounter rcc = new RayCrossingCounter(p);
    
    SegmentVisitor visitor = new SegmentVisitor(rcc);
    index.query(p.y, p.y, visitor);
  
    /*
     // MD - slightly slower alternative
    List segs = index.query(p.y, p.y);
    countSegs(rcc, segs);
    */
    
    return rcc.getLocation();
  }

  /**
   * Creates the indexed geometry, creating it if necessary.
   */
  private synchronized void createIndex() {
    if (index == null) {
      index = new IntervalIndexedGeometry(geom);
      // no need to hold onto geom
      geom = null;
    }
  }
  
  private static class SegmentVisitor
    implements ItemVisitor
  {
    private RayCrossingCounter counter;
    
    public SegmentVisitor(RayCrossingCounter counter)
    {
      this.counter = counter;
    }
    
    public void visitItem(Object item)
    {
      LineSegment seg = (LineSegment) item;
      counter.countSegment(seg.getCoordinate(0), seg.getCoordinate(1));
    }
  }
  
  private static class IntervalIndexedGeometry
  {
    private boolean isEmpty = false;
    private SortedPackedIntervalRTree index= new SortedPackedIntervalRTree();

    public IntervalIndexedGeometry(Geometry geom)
    {
      if (geom.isEmpty())
        isEmpty = true;
      else
        init(geom);
    }
    
    private void init(Geometry geom)
    {
      List lines = LinearComponentExtracter.getLines(geom);
      for (Iterator i = lines.iterator(); i.hasNext(); ) {
        LineString line = (LineString) i.next();
        Coordinate[] pts = line.getCoordinates();
        addLine(pts);
      }
    }
    
    private void addLine(Coordinate[] pts)
    {
      for (int i = 1; i < pts.length; i++) {
        LineSegment seg = new LineSegment(pts[i-1], pts[i]);
        double min = Math.min(seg.p0.y, seg.p1.y);
        double max = Math.max(seg.p0.y, seg.p1.y);
        index.insert(min, max, seg);
      }
    }
    
    public List query(double min, double max)
    {
     if (isEmpty) 
        return new ArrayList();
      
      ArrayListVisitor visitor = new ArrayListVisitor();
      index.query(min, max, visitor);
      return visitor.getItems();
    }
    
    public void query(double min, double max, ItemVisitor visitor)
    {
      if (isEmpty) 
        return;
      index.query(min, max, visitor);
    }
  }

}



