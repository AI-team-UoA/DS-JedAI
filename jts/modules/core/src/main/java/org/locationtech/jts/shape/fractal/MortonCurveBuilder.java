/*
 * Copyright (c) 2019 Martin Davis.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */

package org.locationtech.jts.shape.fractal;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.shape.GeometricShapeBuilder;
import static org.locationtech.jts.shape.fractal.MortonCode.*;

/**
 * Generates a {@link LineString} representing the Morton Curve
 * at a given level.
 * 
 * @author Martin Davis
 * @see MortonCode
 */
public class MortonCurveBuilder
extends GeometricShapeBuilder
{

  /**
   * Creates a new instance using the provided {@link GeometryFactory}.
   * 
   * @param geomFactory the geometry factory to use
   */
  public MortonCurveBuilder(GeometryFactory geomFactory)
  {
    super(geomFactory);
    // use a null extent to indicate no transformation
    // (may be set by client)
    extent = null;
  }

  /**
   * Sets the level of curve to generate.
   * The level must be in the range [0 - 16].
   * This determines the 
   * number of points in the generated curve.
   * 
   * @param level the level of the curve
   */
  public void setLevel(int level) {
    this.numPts = MortonCode.size(level);
  }
  
  @Override
  public Geometry getGeometry() {
    int level = MortonCode.level(numPts);
    int nPts = MortonCode.size(level);
    
    double scale = 1;
    double baseX = 0;
    double baseY = 0;
    if (extent != null) {
      LineSegment baseLine = getSquareBaseLine();
      baseX = baseLine.minX();
      baseY = baseLine.minY();
      double width = baseLine.getLength();
      int maxOrdinate = MortonCode.maxOrdinate(level);
      scale = width / maxOrdinate;
    }
    
    Coordinate[] pts = new Coordinate[nPts];
    for (int i = 0; i < nPts; i++) {
       Coordinate pt = MortonCode.decode(i);
       double x = transform(pt.getX(), scale, baseX);
       double y = transform(pt.getY(), scale, baseY);
       pts[i] = new Coordinate(x, y);
    }
    return geomFactory.createLineString(pts);
  }
  
  private static double transform(double val, double scale, double offset) {
    return val * scale + offset;
  }

}
