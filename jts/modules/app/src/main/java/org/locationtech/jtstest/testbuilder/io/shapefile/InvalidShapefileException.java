/*
 * Copyright (c) 2003 Open Source Geospatial Foundation, All rights reserved.
 * 
 * This program and the accompanying materials are made available under the terms
 * of the OSGeo BSD License v1.0 available at:
 *
 * https://www.osgeo.org/sites/osgeo.org/files/Page/osgeo-bsd-license.txt
 */
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

package org.locationtech.jtstest.testbuilder.io.shapefile;

/**
 * Thrown when an attempt is made to load a shapefile
 * which contains an error such as an invlaid shape
 */
public class InvalidShapefileException extends ShapefileException{
    public InvalidShapefileException(String s){
        super(s);
    }
}
