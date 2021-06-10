/*
 * Copyright (c) 2016 Vivid Solutions.
 * Copyright (c) 2020 Martin Davis.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */

package org.locationtech.jts.noding.snapround;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.LinearComponentExtracter;
import org.locationtech.jts.noding.NodedSegmentString;
import org.locationtech.jts.noding.Noder;
import org.locationtech.jts.noding.NodingValidator;
import org.locationtech.jts.noding.SegmentString;
import org.locationtech.jts.geom.util.LinearComponentExtracter;

/**
 * Nodes the linework in a list of {@link Geometry}s using Snap-Rounding
 * to a given {@link PrecisionModel}.
 * <p>
 * Input coordinates do not need to be rounded to the 
 * precision model.  
 * All output coordinates are rounded to the precision model.
 * <p>
 * This class does <b>not</b> dissolve the output linework,
 * so there may be duplicate linestrings in the output.  
 * Subsequent processing (e.g. polygonization) may require
 * the linework to be unique.  Using <code>UnaryUnion</code> is one way
 * to do this (although this is an inefficient approach).
 * 
 */
public class GeometryNoder
{
  private GeometryFactory geomFact;
  private PrecisionModel pm;
  private boolean isValidityChecked = false;

  /**
   * Creates a new noder which snap-rounds to a grid specified
   * by the given {@link PrecisionModel}.
   * 
   * @param pm the precision model for the grid to snap-round to
   */
  public GeometryNoder(PrecisionModel pm) {
    this.pm = pm;
  }

  /**
   * Sets whether noding validity is checked after noding is performed.
   * 
   * @param isValidityChecked
   */
  public void setValidate(boolean isValidityChecked)
  {
  	this.isValidityChecked = isValidityChecked;
  }
  
  /**
   * Nodes the linework of a set of Geometrys using SnapRounding. 
   * 
   * @param geoms a Collection of Geometrys of any type
   * @return a List of LineStrings representing the noded linework of the input
   */
  public List node(Collection geoms)
  {
    // get geometry factory
    Geometry geom0 = (Geometry) geoms.iterator().next();
    geomFact = geom0.getFactory();

    List segStrings = toSegmentStrings(extractLines(geoms));
    Noder sr = new SnapRoundingNoder(pm);
    sr.computeNodes(segStrings);
    Collection nodedLines = sr.getNodedSubstrings();

    //TODO: improve this to check for full snap-rounded correctness
    if (isValidityChecked) {
    	NodingValidator nv = new NodingValidator(nodedLines);
    	nv.checkValid();
    }

    return toLineStrings(nodedLines);
  }

  private List toLineStrings(Collection segStrings)
  {
    List lines = new ArrayList();
    for (Iterator it = segStrings.iterator(); it.hasNext(); ) {
      SegmentString ss = (SegmentString) it.next();
      // skip collapsed lines
      if (ss.size() < 2)
      	continue;
      lines.add(geomFact.createLineString(ss.getCoordinates()));
    }
    return lines;
  }

  private List extractLines(Collection geoms)
  {
    List lines = new ArrayList();
    LinearComponentExtracter lce = new LinearComponentExtracter(lines);
    for (Iterator it = geoms.iterator(); it.hasNext(); ) {
      Geometry geom = (Geometry) it.next();
      geom.apply(lce);
    }
    return lines;
  }

  private List toSegmentStrings(Collection lines)
  {
    List segStrings = new ArrayList();
    for (Iterator it = lines.iterator(); it.hasNext(); ) {
      LineString line = (LineString) it.next();
      segStrings.add(new NodedSegmentString(line.getCoordinates(), null));
    }
    return segStrings;
  }
}
