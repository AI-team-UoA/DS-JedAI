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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.GeometryTransformer;

/**
 * Simplifies a {@link Geometry} using the Douglas-Peucker algorithm.
 * Ensures that any polygonal geometries returned are valid.
 * Simple lines are not guaranteed to remain simple after simplification.
 * All geometry types are handled. 
 * Empty and point geometries are returned unchanged.
 * Empty geometry components are deleted.
 * <p>
 * Note that in general D-P does not preserve topology -
 * e.g. polygons can be split, collapse to lines or disappear
 * holes can be created or disappear,
 * and lines can cross.
 * To simplify geometry while preserving topology use {@link TopologyPreservingSimplifier}.
 * (However, using D-P is significantly faster).
 *<h2>KNOWN BUGS</h2>
 *<ul>
 *<li>In some cases the approach used to clean invalid simplified polygons
 *can distort the output geometry severely.
 *</ul>
 *
 *
 * @version 1.7
 * @see TopologyPreservingSimplifier
 */
public class DouglasPeuckerSimplifier
{

  /**
   * Simplifies a geometry using a given tolerance.
   * 
   * @param geom geometry to simplify
   * @param distanceTolerance the tolerance to use
   * @return a simplified version of the geometry
   */
  public static Geometry simplify(Geometry geom, double distanceTolerance)
  {
    DouglasPeuckerSimplifier tss = new DouglasPeuckerSimplifier(geom);
    tss.setDistanceTolerance(distanceTolerance);
    return tss.getResultGeometry();
  }

  private Geometry inputGeom;
  private double distanceTolerance;
  private boolean isEnsureValidTopology = true;
  
  /**
   * Creates a simplifier for a given geometry.
   * 
   * @param inputGeom the geometry to simplify
   */
  public DouglasPeuckerSimplifier(Geometry inputGeom)
  {
    this.inputGeom = inputGeom;
  }

  /**
   * Sets the distance tolerance for the simplification.
   * All vertices in the simplified geometry will be within this
   * distance of the original geometry.
   * The tolerance value must be non-negative. 
   *
   * @param distanceTolerance the approximation tolerance to use
   */
  public void setDistanceTolerance(double distanceTolerance) {
    if (distanceTolerance < 0.0)
      throw new IllegalArgumentException("Tolerance must be non-negative");
    this.distanceTolerance = distanceTolerance;
  }

  /**
   * Controls whether simplified polygons will be "fixed"
   * to have valid topology.
   * The caller may choose to disable this because:
   * <ul>
   * <li>valid topology is not required
   * <li>fixing topology is a relative expensive operation
   * <li>in some pathological cases the topology fixing operation may either fail or run for too long
   * </ul>
   * 
   * The default is to fix polygon topology.
   * 
   * @param isEnsureValidTopology
   */
  public void setEnsureValid(boolean isEnsureValidTopology)
  {
  	this.isEnsureValidTopology = isEnsureValidTopology;
  }
  
  /**
   * Gets the simplified geometry.
   * 
   * @return the simplified geometry
   */
  public Geometry getResultGeometry()
  {
    // empty input produces an empty result
    if (inputGeom.isEmpty()) return inputGeom.copy();
    
    return (new DPTransformer(isEnsureValidTopology, distanceTolerance)).transform(inputGeom);
  }

static class DPTransformer
    extends GeometryTransformer
{
  private boolean isEnsureValidTopology = true;
  private double distanceTolerance;

	public DPTransformer(boolean isEnsureValidTopology, double distanceTolerance)
	{
		this.isEnsureValidTopology = isEnsureValidTopology;
		this.distanceTolerance = distanceTolerance;
	}
	
  protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent)
  {
    Coordinate[] inputPts = coords.toCoordinateArray();
    Coordinate[] newPts = null;
    if (inputPts.length == 0) {
      newPts = new Coordinate[0];
    }
    else {
      newPts = DouglasPeuckerLineSimplifier.simplify(inputPts, distanceTolerance);
    }
    return factory.getCoordinateSequenceFactory().create(newPts);
  }

  /**
   * Simplifies a polygon, fixing it if required.
   */
  protected Geometry transformPolygon(Polygon geom, Geometry parent) {
    // empty geometries are simply removed
    if (geom.isEmpty())
      return null;
    Geometry rawGeom = super.transformPolygon(geom, parent);
    // don't try and correct if the parent is going to do this
    if (parent instanceof MultiPolygon) {
      return rawGeom;
    }
    return createValidArea(rawGeom);
  }

  /**
   * Simplifies a LinearRing.  If the simplification results 
   * in a degenerate ring, remove the component.
   * 
   * @return null if the simplification results in a degenerate ring
   */
  protected Geometry transformLinearRing(LinearRing geom, Geometry parent) 
  {
  	boolean removeDegenerateRings = parent instanceof Polygon;
  	Geometry simpResult = super.transformLinearRing(geom, parent);
  	if (removeDegenerateRings && ! (simpResult instanceof LinearRing))
  		return null;;
  	return simpResult;
  }
  
  /**
   * Simplifies a MultiPolygon, fixing it if required.
   */
  protected Geometry transformMultiPolygon(MultiPolygon geom, Geometry parent) {
    Geometry rawGeom = super.transformMultiPolygon(geom, parent);
    return createValidArea(rawGeom);
  }

  /**
   * Creates a valid area geometry from one that possibly has
   * bad topology (i.e. self-intersections).
   * Since buffer can handle invalid topology, but always returns
   * valid geometry, constructing a 0-width buffer "corrects" the
   * topology.
   * Note this only works for area geometries, since buffer always returns
   * areas.  This also may return empty geometries, if the input
   * has no actual area.  
   * If the input is empty or is not polygonal, 
   * this ensures that POLYGON EMPTY is returned.
   *
   * @param rawAreaGeom an area geometry possibly containing self-intersections
   * @return a valid area geometry
   */
  private Geometry createValidArea(Geometry rawAreaGeom)
  {
    boolean isValidArea = rawAreaGeom.getDimension() == 2 && rawAreaGeom.isValid();
    // if geometry is invalid then make it valid
  	if (isEnsureValidTopology && ! isValidArea)
  		return rawAreaGeom.buffer(0.0);
  	return rawAreaGeom;
  }
}

}


