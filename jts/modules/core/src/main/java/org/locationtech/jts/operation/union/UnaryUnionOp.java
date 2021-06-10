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
package org.locationtech.jts.operation.union;


import java.util.Collection;
import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.geom.Polygon;

/**
 * Unions a <code>Collection</code> of {@link Geometry}s or a single Geometry 
 * (which may be a {@link GeoometryCollection}) together.
 * By using this special-purpose operation over a collection of geometries
 * it is possible to take advantage of various optimizations to improve performance.
 * Heterogeneous {@link GeometryCollection}s are fully supported.
 * <p>
 * The result obeys the following contract:
 * <ul>
 * <li>Unioning a set of {@link Polygon}s has the effect of
 * merging the areas (i.e. the same effect as 
 * iteratively unioning all individual polygons together).
 * 
 * <li>Unioning a set of {@link LineString}s has the effect of <b>noding</b> 
 * and <b>dissolving</b> the input linework.
 * In this context "fully noded" means that there will be 
 * an endpoint or node in the result 
 * for every endpoint or line segment crossing in the input.
 * "Dissolved" means that any duplicate (i.e. coincident) line segments or portions
 * of line segments will be reduced to a single line segment in the result.  
 * This is consistent with the semantics of the 
 * {@link Geometry#union(Geometry)} operation.
 * If <b>merged</b> linework is required, the {@link LineMerger} class can be used.
 * 
 * <li>Unioning a set of {@link Point}s has the effect of merging
 * all identical points (producing a set with no duplicates).
 * </ul>
 * 
 * <tt>UnaryUnion</tt> always operates on the individual components of MultiGeometries.
 * So it is possible to use it to "clean" invalid self-intersecting MultiPolygons
 * (although the polygon components must all still be individually valid.)
 * 
 * @author mbdavis
 *
 */
public class UnaryUnionOp 
{
	/**
	 * Computes the geometric union of a {@link Collection} 
	 * of {@link Geometry}s.
	 * 
	 * @param geoms a collection of geometries
	 * @return the union of the geometries, 
	 * or <code>null</code> if the input is empty
	 */
	public static Geometry union(Collection geoms)
	{
		UnaryUnionOp op = new UnaryUnionOp(geoms);
		return op.union();
	}
	
	/**
	 * Computes the geometric union of a {@link Collection} 
	 * of {@link Geometry}s.
	 * 
	 * If no input geometries were provided but a {@link GeometryFactory} was provided, 
	 * an empty {@link GeometryCollection} is returned.
     *
	 * @param geoms a collection of geometries
	 * @param geomFact the geometry factory to use if the collection is empty
	 * @return the union of the geometries,
	 * or an empty GEOMETRYCOLLECTION
	 */
	public static Geometry union(Collection geoms, GeometryFactory geomFact)
	{
		UnaryUnionOp op = new UnaryUnionOp(geoms, geomFact);
		return op.union();
	}
	
	/**
	 * Constructs a unary union operation for a {@link Geometry}
	 * (which may be a {@link GeometryCollection}).
	 * 
	 * @param geom a geometry to union
	 * @return the union of the elements of the geometry
	 * or an empty GEOMETRYCOLLECTION
	 */
	public static Geometry union(Geometry geom)
	{
		UnaryUnionOp op = new UnaryUnionOp(geom);
		return op.union();
	}
	
	private GeometryFactory geomFact = null;

  private InputExtracter extracter;
  private UnionStrategy unionFunction = CascadedPolygonUnion.CLASSIC_UNION;

	/**
	 * Constructs a unary union operation for a {@link Collection} 
	 * of {@link Geometry}s.
	 * 
	 * @param geoms a collection of geometries
	 * @param geomFact the geometry factory to use if the collection is empty
	 */
	public UnaryUnionOp(Collection geoms, GeometryFactory geomFact)
	{
		this.geomFact = geomFact;
		extract(geoms);
	}
	
	/**
	 * Constructs a unary union operation for a {@link Collection} 
	 * of {@link Geometry}s, using the {@link GeometryFactory}
	 * of the input geometries.
	 * 
	 * @param geoms a collection of geometries
	 */
	public UnaryUnionOp(Collection geoms)
	{
		extract(geoms);
	}
	
	/**
	 * Constructs a unary union operation for a {@link Geometry}
	 * (which may be a {@link GeometryCollection}).
	 * @param geom
	 */
	public UnaryUnionOp(Geometry geom)
	{
		extract(geom);
	}
	
	public void setUnionFunction(UnionStrategy unionFun) {
	  this.unionFunction = unionFun;
	}
	
	private void extract(Collection geoms)
	{
	  extracter = InputExtracter.extract(geoms);
	}
	
	private void extract(Geometry geom)
	{
		extracter = InputExtracter.extract(geom);
	}

	/**
	 * Gets the union of the input geometries.
	 * <p>
	 * The result of empty input is determined as follows:
	 * <ol>
	 * <li>If the input is empty and a dimension can be
	 * determined (i.e. an empty geometry is present), 
	 * an empty atomic geometry of that dimension is returned.
	 * <li>If no input geometries were provided but a {@link GeometryFactory} was provided, 
	 * an empty {@link GeometryCollection} is returned.
	 * <li>Otherwise, the return value is <code>null</code>.
	 * </ol>
	 * 
	 * @return a Geometry containing the union,
	 * or an empty atomic geometry, or an empty GEOMETRYCOLLECTION,
	 * or <code>null</code> if no GeometryFactory was provided
	 */
	public Geometry union()
	{
	  if (geomFact == null)
	    geomFact = extracter.getFactory();
	  
	  // Case 3
		if (geomFact == null) {
			return null;
		}
		
		// Case 1 & 2
		if (extracter.isEmpty()) {
		  return geomFact.createEmpty( extracter.getDimension() );
		}
    List points = extracter.getExtract(0);
    List lines = extracter.getExtract(1);
    List polygons = extracter.getExtract(2);
    
		/**
		 * For points and lines, only a single union operation is 
		 * required, since the OGC model allows self-intersecting
		 * MultiPoint and MultiLineStrings.
		 * This is not the case for polygons, so Cascaded Union is required.
		 */
		Geometry unionPoints = null;
		if (points.size() > 0) {
			Geometry ptGeom = geomFact.buildGeometry(points);
			unionPoints = unionNoOpt(ptGeom);
		}
		
		Geometry unionLines = null;
		if (lines.size() > 0) {
			Geometry lineGeom = geomFact.buildGeometry(lines);
			unionLines = unionNoOpt(lineGeom);
		}
		
		Geometry unionPolygons = null;
		if (polygons.size() > 0) {
			unionPolygons = CascadedPolygonUnion.union(polygons, unionFunction);
		}
		
    /**
     * Performing two unions is somewhat inefficient,
     * but is mitigated by unioning lines and points first
     */
		Geometry unionLA = unionWithNull(unionLines, unionPolygons);
		Geometry union = null;
		if (unionPoints == null)
			union = unionLA;
		else if (unionLA == null)
			union = unionPoints;
		else 
			union = PointGeometryUnion.union((Puntal) unionPoints, unionLA);
		
		if (union == null)
			return geomFact.createGeometryCollection();
		
		return union;
	}
	
  /**
   * Computes the union of two geometries, 
   * either of both of which may be null.
   * 
   * @param g0 a Geometry
   * @param g1 a Geometry
   * @return the union of the input(s)
   * or null if both inputs are null
   */
  private Geometry unionWithNull(Geometry g0, Geometry g1)
  {
  	if (g0 == null && g1 == null)
  		return null;

  	if (g1 == null)
  		return g0;
  	if (g0 == null)
  		return g1;
  	
  	return g0.union(g1);
  }

  /**
   * Computes a unary union with no extra optimization,
   * and no short-circuiting.
   * Due to the way the overlay operations 
   * are implemented, this is still efficient in the case of linear 
   * and puntal geometries.
   * Uses robust version of overlay operation
   * to ensure identical behaviour to the <tt>union(Geometry)</tt> operation.
   * 
   * @param g0 a geometry
   * @return the union of the input geometry
   */
	private Geometry unionNoOpt(Geometry g0)
	{
    Geometry empty = geomFact.createPoint();
		return unionFunction.union(g0, empty);
	}
	
}
