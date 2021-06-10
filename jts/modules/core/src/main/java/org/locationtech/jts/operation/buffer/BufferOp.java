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
package org.locationtech.jts.operation.buffer;

/**
 * @version 1.7
 */
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.math.MathUtil;
import org.locationtech.jts.noding.Noder;
import org.locationtech.jts.noding.ScaledNoder;
import org.locationtech.jts.noding.snapround.SnapRoundingNoder;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.math.MathUtil;
import org.locationtech.jts.noding.snapround.SnapRoundingNoder;

//import debug.*;

/**
 * Computes the buffer of a geometry, for both positive and negative buffer distances.
 * <p>
 * In GIS, the positive (or negative) buffer of a geometry is defined as
 * the Minkowski sum (or difference) of the geometry
 * with a circle of radius equal to the absolute value of the buffer distance.
 * In the CAD/CAM world buffers are known as <i>offset curves</i>.
 * In morphological analysis the 
 * operation of positive and negative buffering 
 * is referred to as <i>erosion</i> and <i>dilation</i>
 * <p>
 * The buffer operation always returns a polygonal result.
 * The negative or zero-distance buffer of lines and points is always an empty {@link Polygon}.
 * <p>
 * Since true buffer curves may contain circular arcs,
 * computed buffer polygons are only approximations to the true geometry.
 * The user can control the accuracy of the approximation by specifying
 * the number of linear segments used to approximate arcs.
 * This is specified via {@link BufferParameters#setQuadrantSegments(int)} or {@link #setQuadrantSegments(int)}.
 * <p>
 * The <b>end cap style</b> of a linear buffer may be {@link BufferParameters#setEndCapStyle(int) specified}. The
 * following end cap styles are supported:
 * <ul>
 * <li>{@link BufferParameters#CAP_ROUND} - the usual round end caps
 * <li>{@link BufferParameters#CAP_FLAT} - end caps are truncated flat at the line ends
 * <li>{@link BufferParameters#CAP_SQUARE} - end caps are squared off at the buffer distance beyond the line ends
 * </ul>
 * <p>
 * The <b>join style</b> of the corners in a buffer may be {@link BufferParameters#setJoinStyle(int) specified}. The
 * following join styles are supported:
 * <ul>
 * <li>{@link BufferParameters#JOIN_ROUND} - the usual round join
 * <li>{@link BufferParameters#JOIN_MITRE} - corners are "sharp" (up to a {@link BufferParameters#getMitreLimit() distance limit})
 * <li>{@link BufferParameters#JOIN_BEVEL} - corners are beveled (clipped off).
 * </ul>
 * <p>
 * The buffer algorithm may perform simplification on the input to increase performance.
 * The simplification is performed a way that always increases the buffer area 
 * (so that the simplified input covers the original input).
 * The degree of simplification can be {@link BufferParameters#setSimplifyFactor(double) specified},
 * with a {@link BufferParameters#DEFAULT_SIMPLIFY_FACTOR default} used otherwise.
 * Note that if the buffer distance is zero then so is the computed simplify tolerance, 
 * no matter what the simplify factor.
 * <p>
 * Buffer results are always valid geometry.
 * Given this, computing a zero-width buffer of an invalid polygonal geometry is
 * an effective way to "validify" the geometry.
 * Note however that in the case of self-intersecting "bow-tie" geometries,
 * only the largest enclosed area will be retained.
 *
 * @version 1.7
 */
public class BufferOp
{
  /**
   * Specifies a round line buffer end cap style.
   * @deprecated use BufferParameters
   */
  public static final int CAP_ROUND = BufferParameters.CAP_ROUND;
  /**
   * Specifies a butt (or flat) line buffer end cap style.
   * @deprecated use BufferParameters
   */
  public static final int CAP_BUTT = BufferParameters.CAP_FLAT;
  
  /**
   * Specifies a butt (or flat) line buffer end cap style.
   * @deprecated use BufferParameters
   */
  public static final int CAP_FLAT = BufferParameters.CAP_FLAT;
  /**
   * Specifies a square line buffer end cap style.
   * @deprecated use BufferParameters
   */
  public static final int CAP_SQUARE = BufferParameters.CAP_SQUARE;
  
  /**
   * A number of digits of precision which leaves some computational "headroom"
   * for floating point operations.
   * 
   * This value should be less than the decimal precision of double-precision values (16).
   */
  private static int MAX_PRECISION_DIGITS = 12;

  /**
   * Compute a scale factor to limit the precision of
   * a given combination of Geometry and buffer distance.
   * The scale factor is determined by
   * the number of digits of precision in the (geometry + buffer distance),
   * limited by the supplied <code>maxPrecisionDigits</code> value.
   * <p>
   * The scale factor is based on the absolute magnitude of the (geometry + buffer distance).
   * since this determines the number of digits of precision which must be handled.
   *
   * @param g the Geometry being buffered
   * @param distance the buffer distance
   * @param maxPrecisionDigits the max # of digits that should be allowed by
   *          the precision determined by the computed scale factor
   *
   * @return a scale factor for the buffer computation
   */
  private static double precisionScaleFactor(Geometry g,
      double distance,
    int maxPrecisionDigits)
  {
    Envelope env = g.getEnvelopeInternal();
    double envMax = MathUtil.max(
        Math.abs(env.getMaxX()), 
            Math.abs(env.getMaxY()), 
                Math.abs(env.getMinX()), 
                    Math.abs(env.getMinY())
            );
    
    double expandByDistance = distance > 0.0 ? distance : 0.0;
    double bufEnvMax = envMax + 2 * expandByDistance;

    // the smallest power of 10 greater than the buffer envelope
    int bufEnvPrecisionDigits = (int) (Math.log(bufEnvMax) / Math.log(10) + 1.0);
    int minUnitLog10 = maxPrecisionDigits - bufEnvPrecisionDigits;
    
    double scaleFactor = Math.pow(10.0, minUnitLog10);
    return scaleFactor;
  }

  /*
  private static double OLDprecisionScaleFactor(Geometry g,
      double distance,
    int maxPrecisionDigits)
  {
    Envelope env = g.getEnvelopeInternal();
    double envSize = Math.max(env.getHeight(), env.getWidth());
    double expandByDistance = distance > 0.0 ? distance : 0.0;
    double bufEnvSize = envSize + 2 * expandByDistance;

    // the smallest power of 10 greater than the buffer envelope
    int bufEnvLog10 = (int) (Math.log(bufEnvSize) / Math.log(10) + 1.0);
    int minUnitLog10 = bufEnvLog10 - maxPrecisionDigits;
    // scale factor is inverse of min Unit size, so flip sign of exponent
    double scaleFactor = Math.pow(10.0, -minUnitLog10);
    return scaleFactor;
  }
  */

  /**
   * Computes the buffer of a geometry for a given buffer distance.
   *
   * @param g the geometry to buffer
   * @param distance the buffer distance
   * @return the buffer of the input geometry
   */
  public static Geometry bufferOp(Geometry g, double distance)
  {
    BufferOp gBuf = new BufferOp(g);
    Geometry geomBuf = gBuf.getResultGeometry(distance);
//BufferDebug.saveBuffer(geomBuf);
    //BufferDebug.runCount++;
    return geomBuf;
  }

  /**
   * Computes the buffer for a geometry for a given buffer distance
   * and accuracy of approximation.
   *
   * @param g the geometry to buffer
   * @param distance the buffer distance
   * @param params the buffer parameters to use
   * @return the buffer of the input geometry
   *
   */
  public static Geometry bufferOp(Geometry g, double distance, BufferParameters params)
  {
    BufferOp bufOp = new BufferOp(g, params);
    Geometry geomBuf = bufOp.getResultGeometry(distance);
    return geomBuf;
  }
  
  /**
   * Computes the buffer for a geometry for a given buffer distance
   * and accuracy of approximation.
   *
   * @param g the geometry to buffer
   * @param distance the buffer distance
   * @param quadrantSegments the number of segments used to approximate a quarter circle
   * @return the buffer of the input geometry
   *
   */
  public static Geometry bufferOp(Geometry g, double distance, int quadrantSegments)
  {
    BufferOp bufOp = new BufferOp(g);
    bufOp.setQuadrantSegments(quadrantSegments);
    Geometry geomBuf = bufOp.getResultGeometry(distance);
    return geomBuf;
  }

  /**
   * Computes the buffer for a geometry for a given buffer distance
   * and accuracy of approximation.
   *
   * @param g the geometry to buffer
   * @param distance the buffer distance
   * @param quadrantSegments the number of segments used to approximate a quarter circle
   * @param endCapStyle the end cap style to use
   * @return the buffer of the input geometry
   *
   */
  public static Geometry bufferOp(Geometry g,
                                  double distance,
    int quadrantSegments,
    int endCapStyle)
  {
    BufferOp bufOp = new BufferOp(g);
    bufOp.setQuadrantSegments(quadrantSegments);
    bufOp.setEndCapStyle(endCapStyle);
    Geometry geomBuf = bufOp.getResultGeometry(distance);
    return geomBuf;
  }

  private Geometry argGeom;
  private double distance;
  
  private BufferParameters bufParams = new BufferParameters();

  private Geometry resultGeometry = null;
  private RuntimeException saveException;   // debugging only

  /**
   * Initializes a buffer computation for the given geometry
   *
   * @param g the geometry to buffer
   */
  public BufferOp(Geometry g) {
    argGeom = g;
  }

  /**
   * Initializes a buffer computation for the given geometry
   * with the given set of parameters
   *
   * @param g the geometry to buffer
   * @param bufParams the buffer parameters to use
   */
  public BufferOp(Geometry g, BufferParameters bufParams) {
    argGeom = g;
    this.bufParams = bufParams;
  }

  /**
   * Specifies the end cap style of the generated buffer.
   * The styles supported are {@link BufferParameters#CAP_ROUND}, {@link BufferParameters#CAP_FLAT}, and {@link BufferParameters#CAP_SQUARE}.
   * The default is CAP_ROUND.
   *
   * @param endCapStyle the end cap style to specify
   */
  public void setEndCapStyle(int endCapStyle)
  {
    bufParams.setEndCapStyle(endCapStyle);
  }

  /**
   * Sets the number of segments used to approximate a angle fillet
   *
   * @param quadrantSegments the number of segments in a fillet for a quadrant
   */
  public void setQuadrantSegments(int quadrantSegments)
  {
    bufParams.setQuadrantSegments(quadrantSegments);
  }

  /**
   * Returns the buffer computed for a geometry for a given buffer distance.
   *
   * @param distance the buffer distance
   * @return the buffer of the input geometry
   */
  public Geometry getResultGeometry(double distance)
  {
    this.distance = distance;
    computeGeometry();
    return resultGeometry;
  }

  private void computeGeometry()
  {
    bufferOriginalPrecision();
    if (resultGeometry != null) return;

    PrecisionModel argPM = argGeom.getFactory().getPrecisionModel();
    if (argPM.getType() == PrecisionModel.FIXED)
      bufferFixedPrecision(argPM);
    else
      bufferReducedPrecision();
  }

  private void bufferReducedPrecision()
  {
    // try and compute with decreasing precision
    for (int precDigits = MAX_PRECISION_DIGITS; precDigits >= 0; precDigits--) {
      try {
        bufferReducedPrecision(precDigits);
      }
      catch (TopologyException ex) {
      	// update the saved exception to reflect the new input geometry
        saveException = ex;
        // don't propagate the exception - it will be detected by fact that resultGeometry is null
      }
      if (resultGeometry != null) return;
    }

    // tried everything - have to bail
    throw saveException;
  }

  private void bufferReducedPrecision(int precisionDigits)
  {
    double sizeBasedScaleFactor = precisionScaleFactor(argGeom, distance, precisionDigits);
//    System.out.println("recomputing with precision scale factor = " + sizeBasedScaleFactor);

    PrecisionModel fixedPM = new PrecisionModel(sizeBasedScaleFactor);
    bufferFixedPrecision(fixedPM);
  }
  
  private void bufferOriginalPrecision()
  {
    try {
      // use fast noding by default
      BufferBuilder bufBuilder = new BufferBuilder(bufParams);
      resultGeometry = bufBuilder.buffer(argGeom, distance);
    }
    catch (RuntimeException ex) {
      saveException = ex;
      // don't propagate the exception - it will be detected by fact that resultGeometry is null

      // testing ONLY - propagate exception
      //throw ex;
    }
  }

  private void bufferFixedPrecision(PrecisionModel fixedPM)
  {
    //System.out.println("recomputing with precision scale factor = " + fixedPM);

    /*
     * Snap-Rounding provides both robustness
     * and a fixed output precision.
     * 
     * SnapRoundingNoder does not require rounded input, 
     * so could be used by itself.
     * But using ScaledNoder may be faster, since it avoids
     * rounding within SnapRoundingNoder.
     * (Note this only works for buffering, because
     * ScaledNoder may invalidate topology.)
     */
    Noder snapNoder = new SnapRoundingNoder(new PrecisionModel(1.0));
    Noder noder = new ScaledNoder(snapNoder, fixedPM.getScale());

    BufferBuilder bufBuilder = new BufferBuilder(bufParams);
    bufBuilder.setWorkingPrecisionModel(fixedPM);
    bufBuilder.setNoder(noder);
    // this may throw an exception, if robustness errors are encountered
    resultGeometry = bufBuilder.buffer(argGeom, distance);
  }
}
