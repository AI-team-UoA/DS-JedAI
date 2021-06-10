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

package org.locationtech.jts.linearref;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.util.Assert;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.util.Assert;

/**
 * Computes the length index of the point
 * on a linear {@link Geometry} nearest a given {@link Coordinate}.
 * The nearest point is not necessarily unique; this class
 * always computes the nearest point closest to
 * the start of the geometry.
 */
class LengthIndexOfPoint
{
  public static double indexOf(Geometry linearGeom, Coordinate inputPt)
  {
    LengthIndexOfPoint locater = new LengthIndexOfPoint(linearGeom);
    return locater.indexOf(inputPt);
  }

  public static double indexOfAfter(Geometry linearGeom, Coordinate inputPt, double minIndex)
  {
    LengthIndexOfPoint locater = new LengthIndexOfPoint(linearGeom);
    return locater.indexOfAfter(inputPt, minIndex);
  }

  private Geometry linearGeom;

  public LengthIndexOfPoint(Geometry linearGeom) {
    this.linearGeom = linearGeom;
  }

  /**
   * Find the nearest location along a linear {@link Geometry} to a given point.
   *
   * @param inputPt the coordinate to locate
   * @return the location of the nearest point
   */
  public double indexOf(Coordinate inputPt)
  {
    return indexOfFromStart(inputPt, -1.0);
  }

  /**
   * Finds the nearest index along the linear {@link Geometry}
   * to a given {@link Coordinate}
   * after the specified minimum index.
   * If possible the location returned will be strictly greater than the
   * <code>minLocation</code>.
   * If this is not possible, the
   * value returned will equal <code>minLocation</code>.
   * (An example where this is not possible is when
   * minLocation = [end of line] ).
   *
   * @param inputPt the coordinate to locate
   * @param minIndex the minimum location for the point location
   * @return the location of the nearest point
   */
  public double indexOfAfter(Coordinate inputPt, double minIndex)
  {
    if (minIndex < 0.0) return indexOf(inputPt);

    // sanity check for minIndex at or past end of line
    double endIndex = linearGeom.getLength();
    if (endIndex < minIndex)
      return endIndex;

    double closestAfter = indexOfFromStart(inputPt, minIndex);
    /**
     * Return the minDistanceLocation found.
     */
    Assert.isTrue(closestAfter >= minIndex,
                  "computed index is before specified minimum index");
    return closestAfter;
  }

  private double indexOfFromStart(Coordinate inputPt, double minIndex)
  {
    double minDistance = Double.MAX_VALUE;

    double ptMeasure = minIndex;
    double segmentStartMeasure = 0.0;
    LineSegment seg = new LineSegment();
    LinearIterator it = new LinearIterator(linearGeom);
    while (it.hasNext()) {
      if (! it.isEndOfLine()) {
        seg.p0 = it.getSegmentStart();
        seg.p1 = it.getSegmentEnd();
        double segDistance = seg.distance(inputPt);
        double segMeasureToPt = segmentNearestMeasure(seg, inputPt, segmentStartMeasure);
        if (segDistance < minDistance
            && segMeasureToPt > minIndex) {
          ptMeasure = segMeasureToPt;
          minDistance = segDistance;
        }
        segmentStartMeasure += seg.getLength();
      }
      it.next();
    }
    return ptMeasure;
  }

  private double segmentNearestMeasure(LineSegment seg, Coordinate inputPt,
                            double segmentStartMeasure)
  {
    // found new minimum, so compute location distance of point
    double projFactor = seg.projectionFactor(inputPt);
    if (projFactor <= 0.0)
      return segmentStartMeasure;
    if (projFactor <= 1.0)
      return segmentStartMeasure + projFactor * seg.getLength();
    // projFactor > 1.0
    return segmentStartMeasure + seg.getLength();
  }
}
