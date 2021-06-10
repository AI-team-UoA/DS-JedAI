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

/**
 * Utility functions for working with quadrants of the Euclidean plane.
 * <p>
 * Quadrants are referenced and numbered as follows:
 * <pre>
 * 1 - NW | 0 - NE
 * -------+-------
 * 2 - SW | 3 - SE
 * </pre>
 *
 * @version 1.7
 */
public class Quadrant 
{
	public static final int NE = 0;
	public static final int NW = 1;
	public static final int SW = 2;
	public static final int SE = 3;
	
  /**
   * Returns the quadrant of a directed line segment (specified as x and y
   * displacements, which cannot both be 0).
   * 
   * @throws IllegalArgumentException if the displacements are both 0
   */
  public static int quadrant(double dx, double dy)
  {
    if (dx == 0.0 && dy == 0.0)
      throw new IllegalArgumentException("Cannot compute the quadrant for point ( "+ dx + ", " + dy + " )" );
    if (dx >= 0.0) {
      if (dy >= 0.0)
        return NE;
      else
        return SE;
    }
    else {
    	if (dy >= 0.0)
    		return NW;
    	else
    		return SW;
    }
  }

  /**
   * Returns the quadrant of a directed line segment from p0 to p1.
   * 
   * @throws IllegalArgumentException if the points are equal
   */
  public static int quadrant(Coordinate p0, Coordinate p1)
  {
    if (p1.x == p0.x && p1.y == p0.y)
      throw new IllegalArgumentException("Cannot compute the quadrant for two identical points " + p0);
    
    if (p1.x >= p0.x) {
      if (p1.y >= p0.y)
        return NE;
      else
        return SE;
    }
    else {
    	if (p1.y >= p0.y)
    		return NW;
    	else
    		return SW;
    }
  }

  /**
   * Returns true if the quadrants are 1 and 3, or 2 and 4
   */
  public static boolean isOpposite(int quad1, int quad2)
  {
    if (quad1 == quad2) return false;
    int diff = (quad1 - quad2 + 4) % 4;
    // if quadrants are not adjacent, they are opposite
    if (diff == 2) return true;
    return false;
  }

  /** 
   * Returns the right-hand quadrant of the halfplane defined by the two quadrants,
   * or -1 if the quadrants are opposite, or the quadrant if they are identical.
   */
  public static int commonHalfPlane(int quad1, int quad2)
  {
    // if quadrants are the same they do not determine a unique common halfplane.
    // Simply return one of the two possibilities
    if (quad1 == quad2) return quad1;
    int diff = (quad1 - quad2 + 4) % 4;
    // if quadrants are not adjacent, they do not share a common halfplane
    if (diff == 2) return -1;
    //
    int min = (quad1 < quad2) ? quad1 : quad2;
    int max = (quad1 > quad2) ? quad1 : quad2;
    // for this one case, the righthand plane is NOT the minimum index;
    if (min == 0 && max == 3) return 3;
    // in general, the halfplane index is the minimum of the two adjacent quadrants
    return min;
  }

  /**
   * Returns whether the given quadrant lies within the given halfplane (specified
   * by its right-hand quadrant).
   */
  public static boolean isInHalfPlane(int quad, int halfPlane)
  {
    if (halfPlane == SE) {
      return quad == SE || quad == SW;
    }
    return quad == halfPlane || quad == halfPlane + 1;
  }
    
  /**
   * Returns true if the given quadrant is 0 or 1.
   */
  public static boolean isNorthern(int quad)
  {
    return quad == NE || quad == NW;
  }
}
