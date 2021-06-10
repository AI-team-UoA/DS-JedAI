

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
package org.locationtech.jtstest.testrunner;

import java.io.File;
import java.util.*;

import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jtstest.geomop.*;



/**
 * @version 1.7
 */
public class TestRun implements Runnable 
{
	// default is to run all cases
	private int testCaseIndexToRun = -1;
  private String description;
  private List<TestCase> testCases = new ArrayList<TestCase>();
  private PrecisionModel precisionModel;
  private GeometryOperation geomOp = null;
  private ResultMatcher resultMatcher = null;
  private int runIndex;
  private File testFile;
  private File workspace;

  /**
   * 
   * @param description
   * @param runIndex
   * @param precisionModel
   * @param geomOp a GeometryOperation to use for all tests in this run (may be null)
   * @param testFile
   */
  public TestRun(String description, 
  		int runIndex, 
  		PrecisionModel precisionModel,
  		GeometryOperation geomOp,
  		ResultMatcher resultMatcher,
      File testFile) {
    this.description = description;
    this.runIndex = runIndex;
    this.precisionModel = precisionModel;
    this.geomOp = geomOp;
    this.resultMatcher = resultMatcher;
    this.testFile = testFile;
  }

  public void setWorkspace(File workspace) {
    this.workspace = workspace;
  }

  public void setTestCaseIndexToRun(int testCaseIndexToRun)
  {
  	this.testCaseIndexToRun = testCaseIndexToRun;
  }
  /**
   * @return null if no workspace set
   */
  public File getWorkspace() {
    return workspace;
  }

  public int getTestCount() {
    int count = 0;
    for (TestCase testCase : testCases) {
      count += testCase.getTestCount();
    }
    return count;
  }

  public String getDescription() {
    return description;
  }

  public int getRunIndex() {
    return runIndex;
  }

  public PrecisionModel getPrecisionModel() {
    return precisionModel;
  }

  public GeometryOperation getGeometryOperation()
  {
  	// use the main one if it was user-specified or this run does not have an op specified
  	if (JTSTestRunnerCmd.isGeometryOperationSpecified()
  			|| geomOp == null)
  		return JTSTestRunnerCmd.getGeometryOperation();
  	
  	return geomOp;
  }
  
  public ResultMatcher getResultMatcher()
  {
  	// use the main one if it was user-specified or this run does not have an op specified
  	if (JTSTestRunnerCmd.isResultMatcherSpecified()
  			|| resultMatcher == null)
  		return JTSTestRunnerCmd.getResultMatcher();
  	
  	return resultMatcher;
  }
  
  public List<TestCase> getTestCases() {
    return Collections.unmodifiableList(testCases);
  }

  public File getTestFile() {
    return testFile;
  }

  public String getTestFileName() {
    if (testFile == null) 
      return "";
    return testFile.getName();
  }

  public void addTestCase(TestCase testCase) {
    testCases.add(testCase);
  }

  public void run() {
    for (TestCase testCase : testCases ) {
      if (testCaseIndexToRun < 0 || testCase.getCaseIndex() == testCaseIndexToRun)
      	testCase.run();
    }
  }

}

