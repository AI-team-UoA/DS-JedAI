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
package org.locationtech.jtstest.geomop;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Geometry;

/**
 * An interface for classes which can determine whether
 * two geometries match, within a given tolerance.
 * 
 * @author mbdavis
 *
 */
public interface GeometryMatcher 
{
	void setTolerance(double tolerance);
	boolean match(Geometry a, Geometry b);
}
