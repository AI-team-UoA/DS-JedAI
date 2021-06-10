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
import org.locationtech.jts.triangulate.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.triangulate.ConformingDelaunayTriangulationBuilder;
import org.locationtech.jts.triangulate.DelaunayTriangulationBuilder;
import org.locationtech.jts.triangulate.VertexTaggedGeometryDataMapper;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;
import org.locationtech.jts.triangulate.quadedge.LocateFailureException;
import org.locationtech.jts.triangulate.quadedge.QuadEdgeSubdivision;
import org.locationtech.jtstest.util.*;


public class TriangulationFunctions 
{
	private static final double TRIANGULATION_TOLERANCE = 0.0;
	
  public static Geometry delaunayEdges(Geometry geom)
  {
    DelaunayTriangulationBuilder builder = new DelaunayTriangulationBuilder();
    builder.setSites(geom);
    builder.setTolerance(TRIANGULATION_TOLERANCE);
    Geometry edges = builder.getEdges(geom.getFactory());
    return edges;
  }

  public static Geometry delaunayTriangles(Geometry geom)
  {
    DelaunayTriangulationBuilder builder = new DelaunayTriangulationBuilder();
    builder.setSites(geom);
    builder.setTolerance(TRIANGULATION_TOLERANCE);
    Geometry tris = builder.getTriangles(geom.getFactory());
    return tris;
  }

  public static Geometry delaunayEdgesWithTolerance(Geometry geom, double tolerance)
  {
    DelaunayTriangulationBuilder builder = new DelaunayTriangulationBuilder();
    builder.setSites(geom);
    builder.setTolerance(tolerance);
    Geometry edges = builder.getEdges(geom.getFactory());
    return edges;
  }


  public static Geometry delaunayTrianglesWithTolerance(Geometry geom, double tolerance)
  {
    DelaunayTriangulationBuilder builder = new DelaunayTriangulationBuilder();
    builder.setSites(geom);
    builder.setTolerance(tolerance);
    Geometry tris = builder.getTriangles(geom.getFactory());
    return tris;
  }

  public static Geometry delaunayTrianglesWithToleranceNoError(Geometry geom, double tolerance)
  {
    DelaunayTriangulationBuilder builder = new DelaunayTriangulationBuilder();
    builder.setSites(geom);
    builder.setTolerance(tolerance);
    try {
      Geometry tris = builder.getTriangles(geom.getFactory());
      return tris;
    }
    catch (LocateFailureException ex) {
      System.out.println(ex);
      // ignore this exception and drop through
    }
    /**
     * Get the triangles created up until the error
     */
    Geometry tris = builder.getSubdivision().getTriangles(geom.getFactory());
    return tris;      
  }

  public static Geometry voronoiDiagram(Geometry sitesGeom, Geometry clipGeom)
  {
    VoronoiDiagramBuilder builder = new VoronoiDiagramBuilder();
    builder.setSites(sitesGeom);
    if (clipGeom != null)
    	builder.setClipEnvelope(clipGeom.getEnvelopeInternal());
    builder.setTolerance(TRIANGULATION_TOLERANCE);
    Geometry diagram = builder.getDiagram(sitesGeom.getFactory()); 
    return diagram;
  }

  public static Geometry voronoiDiagramWithData(Geometry sitesGeom, Geometry clipGeom)
  {
  	GeometryDataUtil.setComponentDataToIndex(sitesGeom);
  	
  	VertexTaggedGeometryDataMapper mapper = new VertexTaggedGeometryDataMapper();
  	mapper.loadSourceGeometries(sitesGeom);
  	
    VoronoiDiagramBuilder builder = new VoronoiDiagramBuilder();
    builder.setSites(mapper.getCoordinates());
    if (clipGeom != null)
    	builder.setClipEnvelope(clipGeom.getEnvelopeInternal());
    builder.setTolerance(TRIANGULATION_TOLERANCE);
    Geometry diagram = builder.getDiagram(sitesGeom.getFactory()); 
    mapper.transferData(diagram);
    return diagram;
  }

  public static Geometry voronoiRelaxation(Geometry sitesGeom, Geometry clipGeom, int nIter) {
    Geometry voronoiPolys = null;
    for (int i = 0; i < nIter; i++) {
      voronoiPolys = voronoiDiagram(sitesGeom, clipGeom);
      sitesGeom = centroids(voronoiPolys);
    }
    return voronoiPolys;
  }

  private static Geometry centroids(Geometry polygons) {
    int npolys = polygons.getNumGeometries();
    Point[] centroids = new Point[npolys];
    for (int i = 0; i < npolys; i++) {
      centroids[i] = polygons.getGeometryN(i).getCentroid();
    }
    return polygons.getFactory().createMultiPoint(centroids);
  }

  public static Geometry conformingDelaunayEdges(Geometry sites, Geometry constraints)
  {
    return conformingDelaunayEdgesWithTolerance(sites, constraints, TRIANGULATION_TOLERANCE);
  }

  public static Geometry conformingDelaunayEdgesWithTolerance(Geometry sites, Geometry constraints, double tol)
  {
    ConformingDelaunayTriangulationBuilder builder = new ConformingDelaunayTriangulationBuilder();
    builder.setSites(sites);
    builder.setConstraints(constraints);
    builder.setTolerance(tol);
    
    GeometryFactory geomFact = sites != null ? sites.getFactory() : constraints.getFactory();
    Geometry tris = builder.getEdges(geomFact);
    return tris;
  }

  public static Geometry conformingDelaunayTriangles(Geometry sites, Geometry constraints)
  {
    return conformingDelaunayTrianglesWithTolerance(sites, constraints, TRIANGULATION_TOLERANCE);
  }
  
  public static Geometry conformingDelaunayTrianglesWithTolerance(Geometry sites, Geometry constraints, double tol)
  {
		ConformingDelaunayTriangulationBuilder builder = new ConformingDelaunayTriangulationBuilder();
  	builder.setSites(sites);
  	builder.setConstraints(constraints);
  	builder.setTolerance(tol);
  	
  	GeometryFactory geomFact = sites != null ? sites.getFactory() : constraints.getFactory();
  	Geometry tris = builder.getTriangles(geomFact);
  	return tris;
  }

}
