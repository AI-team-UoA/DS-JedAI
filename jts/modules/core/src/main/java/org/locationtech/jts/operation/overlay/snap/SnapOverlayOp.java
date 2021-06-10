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

package org.locationtech.jts.operation.overlay.snap;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.overlay.OverlayOp;
import org.locationtech.jts.precision.CommonBitsRemover;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.overlay.OverlayOp;
import org.locationtech.jts.precision.CommonBitsRemover;

/**
 * Performs an overlay operation using snapping and enhanced precision
 * to improve the robustness of the result.
 * This class <i>always</i> uses snapping.  
 * This is less performant than the standard JTS overlay code, 
 * and may even introduce errors which were not present in the original data.
 * For this reason, this class should only be used 
 * if the standard overlay code fails to produce a correct result. 
 *  
 * @author Martin Davis
 * @version 1.7
 */
public class SnapOverlayOp
{
  public static Geometry overlayOp(Geometry g0, Geometry g1, int opCode)
  {
  	SnapOverlayOp op = new SnapOverlayOp(g0, g1);
  	return op.getResultGeometry(opCode);
  }

  public static Geometry intersection(Geometry g0, Geometry g1)
  {
     return overlayOp(g0, g1, OverlayOp.INTERSECTION);
  }

  public static Geometry union(Geometry g0, Geometry g1)
  {
     return overlayOp(g0, g1, OverlayOp.UNION);
  }

  public static Geometry difference(Geometry g0, Geometry g1)
  {
     return overlayOp(g0, g1, OverlayOp.DIFFERENCE);
  }

  public static Geometry symDifference(Geometry g0, Geometry g1)
  {
     return overlayOp(g0, g1, OverlayOp.SYMDIFFERENCE);
  }
  

  private Geometry[] geom = new Geometry[2];
  private double snapTolerance;

  public SnapOverlayOp(Geometry g1, Geometry g2)
  {
    geom[0] = g1;
    geom[1] = g2;
    computeSnapTolerance();
  }
  private void computeSnapTolerance() 
  {
		snapTolerance = GeometrySnapper.computeOverlaySnapTolerance(geom[0], geom[1]);

		// System.out.println("Snap tol = " + snapTolerance);
	}

  public Geometry getResultGeometry(int opCode)
  {
//  	Geometry[] selfSnapGeom = new Geometry[] { selfSnap(geom[0]), selfSnap(geom[1])};
    Geometry[] prepGeom = snap(geom);
    Geometry result = OverlayOp.overlayOp(prepGeom[0], prepGeom[1], opCode);
    return prepareResult(result);	
  }
  
  private Geometry selfSnap(Geometry geom)
  {
    GeometrySnapper snapper0 = new GeometrySnapper(geom);
    Geometry snapGeom = snapper0.snapTo(geom, snapTolerance);
    //System.out.println("Self-snapped: " + snapGeom);
    //System.out.println();
    return snapGeom;
  }
  
  private Geometry[] snap(Geometry[] geom)
  {
    Geometry[] remGeom = removeCommonBits(geom);
  	
  	// MD - testing only
//  	Geometry[] remGeom = geom;
    
    Geometry[] snapGeom = GeometrySnapper.snap(remGeom[0], remGeom[1], snapTolerance);
    // MD - may want to do this at some point, but it adds cycles
//    checkValid(snapGeom[0]);
//    checkValid(snapGeom[1]);

    /*
    System.out.println("Snapped geoms: ");
    System.out.println(snapGeom[0]);
    System.out.println(snapGeom[1]);
    */
    return snapGeom;
  }

  private Geometry prepareResult(Geometry geom)
  {
    cbr.addCommonBits(geom);
    return geom;
  }

  private CommonBitsRemover cbr;

  private Geometry[] removeCommonBits(Geometry[] geom)
  {
    cbr = new CommonBitsRemover();
    cbr.add(geom[0]);
    cbr.add(geom[1]);
    Geometry remGeom[] = new Geometry[2];
    remGeom[0] = cbr.removeCommonBits(geom[0].copy());
    remGeom[1] = cbr.removeCommonBits(geom[1].copy());
    return remGeom;
  }
  
  private void checkValid(Geometry g)
  {
  	if (! g.isValid()) {
  		System.out.println("Snapped geometry is invalid");
  	}
  }
}
