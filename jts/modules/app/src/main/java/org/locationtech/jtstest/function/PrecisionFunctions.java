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

import org.locationtech.jts.geom.*;
import org.locationtech.jts.precision.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.precision.MinimumClearance;
import org.locationtech.jts.precision.SimpleMinimumClearance;
import org.locationtech.jtstest.geomfunction.Metadata;

public class PrecisionFunctions 
{
	
	public static Geometry reducePrecisionPointwise(Geometry geom,
                                                    @Metadata(title="Scale factor")
	    double scaleFactor)
	{
		PrecisionModel pm = new PrecisionModel(scaleFactor);
		Geometry reducedGeom = GeometryPrecisionReducer.reducePointwise(geom, pm);
		return reducedGeom;
	}
	
	public static Geometry reducePrecision(Geometry geom, 
      @Metadata(title="Scale factor")
	    double scaleFactor)
	{
		PrecisionModel pm = new PrecisionModel(scaleFactor);
		Geometry reducedGeom = GeometryPrecisionReducer.reduce(geom, pm);
		return reducedGeom;
	}
	
  public static Geometry minClearanceLine(Geometry g)
  {
    return MinimumClearance.getLine(g);
  }
  
  public static double minClearance(Geometry g)
  {
    return MinimumClearance.getDistance(g);
  }
  
  public static Geometry minClearanceSimpleLine(Geometry g)
  {
    return SimpleMinimumClearance.getLine(g);
  }
  
  public static double minClearanceSimple(Geometry g)
  {
    return SimpleMinimumClearance.getDistance(g);
  }
}
