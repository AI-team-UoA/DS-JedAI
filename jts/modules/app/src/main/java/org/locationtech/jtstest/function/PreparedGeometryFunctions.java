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

package org.locationtech.jtstest.function;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

/**
 * Function to execute PreparedGeometry methods which have optimized behaviour.
 * @author mdavis
 *
 */
public class PreparedGeometryFunctions 
{
  private static PreparedGeometry createPG(Geometry g)
  {
    return (new PreparedGeometryFactory()).create(g);
  }
  
  public static boolean preparedIntersects(Geometry g1, Geometry g2)
  {
    return createPG(g1).intersects(g2);
  }

  public static boolean preparedContains(Geometry g1, Geometry g2)
  {
    return createPG(g1).contains(g2);
  }

  public static boolean preparedContainsProperly(Geometry g1, Geometry g2)
  {
    return createPG(g1).containsProperly(g2);
  }

  public static boolean preparedCovers(Geometry g1, Geometry g2)
  {
    return createPG(g1).covers(g2);
  }

  

}
