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

package org.locationtech.jts;

/**
 * JTS API version information.
 * <p>
 * Versions consist of a 3-part version number: <code>major.minor.patch</code>
 * An optional release status string may be present in the string version of
 * the version.
 *
 * @version 1.7
 */
public class JTSVersion {

  /**
   * The current version number of the JTS API.
   */
  public static final JTSVersion CURRENT_VERSION = new JTSVersion();

  /**
   * The major version number.
   */
  public static final int MAJOR = 1;

  /**
   * The minor version number.
   */
  public static final int MINOR = 18;

  /**
   * The patch version number.
   */
  public static final int PATCH = 2;

  /**
   * An optional string providing further release info (such as "alpha 1");
   */
  private static final String RELEASE_INFO = "SNAPSHOT";

  /**
   * Prints the current JTS version to stdout.
   *
   * @param args the command-line arguments (none are required).
   */
  public static void main(String[] args)
  {
    System.out.println(CURRENT_VERSION);
  }

  private JTSVersion() {
  }

  /**
   * Gets the major number of the release version.
   *
   * @return the major number of the release version.
   */
  public int getMajor() { return MAJOR; }

  /**
   * Gets the minor number of the release version.
   *
   * @return the minor number of the release version.
   */
  public int getMinor() { return MINOR; }

  /**
   * Gets the patch number of the release version.
   *
   * @return the patch number of the release version.
   */
  public int getPatch() { return PATCH; }

  /**
   * Gets the full version number, suitable for display.
   *
   * @return the full version number, suitable for display.
   */
  public String toString()
  {
    String ver = MAJOR + "." + MINOR + "." + PATCH;
    if (RELEASE_INFO != null && RELEASE_INFO.length() > 0)
      return ver + " " + RELEASE_INFO;
    return ver;
  }

}
