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

package org.locationtech.jtstest.testbuilder.model;

import org.locationtech.jts.geom.Geometry;

public class StaticGeometryContainer implements GeometryContainer {

	private Geometry geometry;
	
	public StaticGeometryContainer(Geometry geometry)
	{
		this.geometry = geometry;
	}
	
  public Geometry getGeometry() {
    return geometry;
  }
  
  public void setGeometry(Geometry geom) {
    geometry = geom;;
  }

}
