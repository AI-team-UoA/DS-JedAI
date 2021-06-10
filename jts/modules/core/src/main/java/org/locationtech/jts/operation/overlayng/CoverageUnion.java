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
package org.locationtech.jts.operation.overlayng;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.noding.Noder;
import org.locationtech.jts.noding.SegmentExtractingNoder;
import org.locationtech.jts.geom.Geometry;

/**
 * Unions a valid coverage of polygons or lines
 * in an efficient way.   
 * <p>
 * A valid polygonal coverage is a collection of {@link Polygon}s
 * which satisfy the following conditions:
 * <ol>
 * <li><b>Vector-clean</b> - Line segments within the collection 
 * must either be identical or intersect only at endpoints.
 * <li><b>Non-overlapping</b> - No two polygons
 * may overlap. Equivalently, polygons must be interior-disjoint.
 * </ol>
 * <p>
 * A valid linear coverage is a collection of {@link LineString}s
 * which satisfies the <b>Vector-clean</b> condition.
 * Note that this does not require the LineStrings to be fully noded
 * - i.e. they may contain coincident linework.  
 * Coincident line segments are dissolved by the union.
 * Currently linear output is not merged (this may be added in a future release.)
 * <p>
 * Currently no checking is done to determine whether the input is a valid coverage.
 * This is because coverage validation involves segment intersection detection,
 * which is much more expensive than the union phase.
 * If the input is not a valid coverage 
 * then in some cases this will be detected during processing 
 * and a {@link TopologyException} is thrown.
 * Otherwise, the computation will produce output, but it will be invalid.
 * <p>
 * Unioning a valid coverage implies that no new vertices are created.
 * This means that a precision model does not need to be specified.
 * The precision of the vertices in the output geometry is not changed.
 * 
 * @author Martin Davis
 * 
 * @see SegmentExtractingNoder
 *
 */
public class CoverageUnion 
{
  /**
   * Unions a valid polygonal coverage or linear network.
   * 
   * @param coverage a coverage of polygons or lines
   * @return the union of the coverage
   * 
   * @throws TopologyException in some cases if the coverage is invalid
   */
  public static Geometry union(Geometry coverage) {
    Noder noder = new SegmentExtractingNoder();
    // a precision model is not needed since no noding is done
    return OverlayNG.union(coverage, null, noder );
  }

  private CoverageUnion() {
    // No instantiation for now
  }
}
