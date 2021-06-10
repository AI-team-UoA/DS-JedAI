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

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.precision.CommonBitsOp;
import org.locationtech.jts.precision.EnhancedPrecisionOp;

public class OverlayCommonBitsRemovedFunctions {
	public static Geometry intersection(Geometry a, Geometry b)		{		return op().intersection(a, b);	}
	public static Geometry union(Geometry a, Geometry b)					{		return op().union(a, b);	}
	public static Geometry symDifference(Geometry a, Geometry b)	{		return op().symDifference(a, b);	}
	public static Geometry difference(Geometry a, Geometry b)			{		return op().difference(a, b);	}
	public static Geometry differenceBA(Geometry a, Geometry b)		{		return op().difference(b, a);	}

	private static CommonBitsOp op() { return new CommonBitsOp(true); }
}
