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
package org.locationtech.jtstest.testbuilder;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.FocusEvent;
import java.awt.event.MouseEvent;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingConstants;
import javax.swing.border.BevelBorder;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;

import org.locationtech.jtstest.testbuilder.event.ValidPanelEvent;
import org.locationtech.jtstest.testbuilder.event.ValidPanelListener;
import org.locationtech.jtstest.testbuilder.model.*;
import org.locationtech.jtstest.testbuilder.ui.SwingUtil;



/**
 * @version 1.7
 */
public class TestCasePanel extends JPanel {
  TestCaseEdit testCase;
  //---------------------------------------------
  BorderLayout borderLayout1 = new BorderLayout();
  BorderLayout editFrameLayout = new BorderLayout();
  JPanel editFramePanel = new JPanel();
  GeometryEditPanel editPanel = new GeometryEditPanel();
  ButtonGroup geometryType = new ButtonGroup();
  ButtonGroup editMode = new ButtonGroup();
  ButtonGroup partType = new ButtonGroup();
  Border border4;
  JPanel editGroupPanel = new JPanel();
  JTabbedPane jTabbedPane1 = new JTabbedPane();
  JPanel btnPanel = new JPanel();
  JPanel relateTabPanel = new JPanel();
  JButton btnRunTests = new JButton();
  RelatePanel relatePanel = new RelatePanel();
  BorderLayout borderLayout2 = new BorderLayout();
  //GeometryEditControlPanel editCtlPanel = new GeometryEditControlPanel();
  BorderLayout borderLayout3 = new BorderLayout();
  JPanel jPanel1 = new JPanel();
  JTextField txtDesc = new JTextField();
  GridBagLayout gridBagLayout1 = new GridBagLayout();
  SpatialFunctionPanel spatialFunctionPanel = new SpatialFunctionPanel();
  private int currentTestCaseIndex = 0;
  private int maxTestCaseIndex = 0;
  private boolean initialized = false;
  JPanel casePrecisionModelPanel = new JPanel();
  JPanel namePanel = new JPanel();
  JLabel testCaseIndexLabel = new JLabel();
  GridBagLayout gridBagLayout2 = new GridBagLayout();
  GridBagLayout gridBagLayout3 = new GridBagLayout();
  JLabel precisionModelLabel = new JLabel();
  ValidPanel validPanel = new ValidPanel();
  JPanel statusBarPanel = new JPanel();
  JLabel lblMousePos = new JLabel();
  JLabel lblPrecisionModel = new JLabel();
  ScalarFunctionPanel scalarFunctionPanel = new ScalarFunctionPanel();
  
  JPanel jPanelReveal = new JPanel();
  JSpinner spStretchDist = new JSpinner(new SpinnerNumberModel(5, 0, 99999, 1));
  JCheckBox cbRevealTopo = new JCheckBox();

  private TestBuilderModel tbModel;
  

  /**
   *  Construct the frame
   */
  public TestCasePanel() {
    try {
      jbInit();
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    initialized = true;
  }

  public void setModel(TestBuilderModel tbModel) 
  { 
  	this.tbModel = tbModel; 
  	editPanel.setModel(tbModel);
    // hook up other beans
    //editCtlPanel.setModel(tbModel);

  }
  
  public void setCurrentTestCaseIndex(int currentTestCaseIndex) {
    this.currentTestCaseIndex = currentTestCaseIndex;
    updateTestCaseIndexLabel();
  }

  public void setMaxTestCaseIndex(int maxTestCaseIndex) {
    this.maxTestCaseIndex = maxTestCaseIndex;
    updateTestCaseIndexLabel();
  }

  public GeometryEditPanel getGeometryEditPanel() {
    return editPanel;
  }

  public SpatialFunctionPanel getSpatialFunctionPanel() {
    return spatialFunctionPanel;
  }

  public ScalarFunctionPanel getScalarFunctionPanel() {
    return scalarFunctionPanel;
  }

  void setTestCase(TestCaseEdit testCase) {
    this.testCase = testCase;
    tbModel.getGeometryEditModel().setTestCase(testCase);
    relatePanel.setTestCase(testCase);
//    spatialFunctionPanel.setTestCase(testCase);
    validPanel.setTestCase(testCase);
//    scalarFunctionPanel.setTestCase(testCase);
    txtDesc.setText(testCase.getName());
  }

  void editPanel_mouseMoved(MouseEvent e) {
    String cursorPos = editPanel.cursorLocationString(e.getPoint());
  	lblMousePos.setText(cursorPos);
//    System.out.println(cursorPos);
  }

  void btnRunTests_actionPerformed(ActionEvent e) {
    relatePanel.runTests();
  }

  void editPanel_geometryChanged(GeometryEvent e) {
    relatePanel.clearResults();
//    scalarFunctionPanel.clearResults();
  }
  void validPanel_setHighlightPerformed(ValidPanelEvent e) {
    editPanel.setHighlightPoint(validPanel.getMarkPoint());
    editPanel.forceRepaint();
  }

  void txtDesc_focusLost(FocusEvent e) {
    testCase.setName(txtDesc.getText());
  }

  void jTabbedPane1_stateChanged(ChangeEvent e) 
  {
    boolean isFunction = jTabbedPane1.getSelectedComponent() == spatialFunctionPanel;
    /*
    // don't bother being clever about what user should see
    // code is buggy anyway - next line is checking wrong panel
    // Plus, should now synch Layer List UI when doing this
    
    editPanel.setShowingResult(isFunction);
    editPanel.setShowingGeometryA(! isFunction
         || spatialFunctionPanel.shouldShowGeometryA());
    editPanel.setShowingGeometryB(! isFunction
         || spatialFunctionPanel.shouldShowGeometryB());
*/
    
    editPanel.setHighlightPoint(null);
    if (jTabbedPane1.getSelectedComponent() == validPanel) {
      editPanel.setHighlightPoint(validPanel.getMarkPoint());
    }
    if (initialized) {
      //avoid infinite loop
      if (isFunction)
        JTSTestBuilderFrame.instance().showResultWKTTab();
    }
  }

  public void setPrecisionModelDescription(String description) {
    precisionModelLabel.setText(description);
    lblPrecisionModel.setText(" PM: " + description);
  }

  /**
   *  Component initialization
   */
  private void jbInit() throws Exception {
    //---------------------------------------------------
    border4 = BorderFactory.createBevelBorder(BevelBorder.LOWERED, Color.white,
        Color.white, new Color(93, 93, 93), new Color(134, 134, 134));
    setLayout(borderLayout1);
    editGroupPanel.setLayout(borderLayout3);
    editPanel.addMouseMotionListener(
      new java.awt.event.MouseMotionAdapter() {

        public void mouseMoved(MouseEvent e) {
          editPanel_mouseMoved(e);
        }
        public void mouseDragged(MouseEvent e) {
          editPanel_mouseMoved(e);
        }
      });
    relateTabPanel.setLayout(borderLayout2);
    btnRunTests.setToolTipText("");
    btnRunTests.setText("Run");
    btnRunTests.addActionListener(
      new java.awt.event.ActionListener() {

        public void actionPerformed(ActionEvent e) {
          btnRunTests_actionPerformed(e);
        }
      });    
    validPanel.addValidPanelListener(
        new ValidPanelListener() {
          public void setHighlightPerformed(ValidPanelEvent e) {
            validPanel_setHighlightPerformed(e);
          }
        });
    jPanel1.setLayout(gridBagLayout1);
    txtDesc.addFocusListener(
      new java.awt.event.FocusAdapter() {

        public void focusLost(FocusEvent e) {
          txtDesc_focusLost(e);
        }
      });
    jTabbedPane1.addChangeListener(
      new javax.swing.event.ChangeListener() {

        public void stateChanged(ChangeEvent e) {
          jTabbedPane1_stateChanged(e);
        }
      });
    //testCaseIndexLabel.setBorder(BorderFactory.createLoweredBevelBorder());
    testCaseIndexLabel.setBorder(new EmptyBorder(0,4,0,0));
    testCaseIndexLabel.setToolTipText("");
    testCaseIndexLabel.setText("0 of 0");
    casePrecisionModelPanel.setLayout(gridBagLayout2);
    namePanel.setLayout(gridBagLayout3);
    precisionModelLabel.setBorder(BorderFactory.createLoweredBevelBorder());
    precisionModelLabel.setToolTipText("Precision Model");
    precisionModelLabel.setText("");

    txtDesc.setBackground(Color.white);
    lblMousePos.setBackground(SystemColor.text);
    lblMousePos.setBorder(BorderFactory.createLoweredBevelBorder());
    lblMousePos.setPreferredSize(new Dimension(21, 21));
    lblMousePos.setHorizontalAlignment(SwingConstants.RIGHT);
    lblPrecisionModel.setBackground(SystemColor.text);
    lblPrecisionModel.setBorder(BorderFactory.createLoweredBevelBorder());
//    txtSelectedPoint.setEditable(false);
    lblPrecisionModel.setText("Sel Pt:");
    
    editFramePanel.setLayout(editFrameLayout);
    editFramePanel.add(editPanel, BorderLayout.CENTER);
    editFramePanel.setBorder(BorderFactory.createBevelBorder(1));
    
    add(editGroupPanel, BorderLayout.CENTER);
    editGroupPanel.add(editFramePanel, BorderLayout.CENTER);
    editGroupPanel.add(statusBarPanel, BorderLayout.SOUTH);
 
    cbRevealTopo.setToolTipText("Reveal Topology - visualize topological detail by stretching geometries");
    spStretchDist.setToolTipText("Stretch Distance (pixels)");
    spStretchDist.setMaximumSize(new Dimension(20,20));
    ((JSpinner.DefaultEditor) spStretchDist.getEditor()).getTextField().setColumns(2);
    jPanelReveal.setLayout(new BoxLayout(jPanelReveal, BoxLayout.LINE_AXIS));
    jPanelReveal.add(Box.createHorizontalGlue());
    jPanelReveal.add(cbRevealTopo);
    jPanelReveal.add(spStretchDist);
    jPanelReveal.add(Box.createHorizontalGlue());
    jPanelReveal.setBorder(BorderFactory.createLoweredBevelBorder());


    JButton btnSaveImage = SwingUtil.createButton(
        AppIcons.SAVE_IMAGE, AppStrings.TIP_SAVE_IMAGE,   
        new java.awt.event.ActionListener() {
          public void actionPerformed(ActionEvent e) {
            if (SwingUtil.isCtlKeyPressed(e)) {
              JTSTestBuilder.controller().saveImageAsPNG();
            } else {
              JTSTestBuilder.controller().saveImageToClipboard();
            }
        }});
    
    JPanel panelCase = new JPanel();
    panelCase.setLayout(new BorderLayout());
    panelCase.setBorder(BorderFactory.createLoweredBevelBorder());
    panelCase.add(btnSaveImage, BorderLayout.EAST);
    panelCase.add(testCaseIndexLabel, BorderLayout.WEST);
    
    statusBarPanel.setLayout(new GridLayout(1,4));
    statusBarPanel.add(panelCase);
    //statusBarPanel.add(testCaseIndexLabel);
    statusBarPanel.add(jPanelReveal);
    statusBarPanel.add(lblPrecisionModel);
    statusBarPanel.add(lblMousePos);
    
    add(jTabbedPane1, BorderLayout.WEST);
    //jTabbedPane1.add(editCtlPanel, "Edit");
    jTabbedPane1.setOpaque(true);
    jTabbedPane1.setBackground(AppColors.BACKGROUND);
    
    JTabbedPane tabFunctions = new JTabbedPane();
    tabFunctions.setOpaque(true);
    tabFunctions.setBackground(AppColors.BACKGROUND);
    tabFunctions.add(spatialFunctionPanel,  "Geometry");
    tabFunctions.add(scalarFunctionPanel,   "Scalar");
    
    jTabbedPane1.add(tabFunctions, "Functions");
    jTabbedPane1.add(relateTabPanel, "Predicates");
    jTabbedPane1.add(validPanel, "Valid / Mark");

    
    relateTabPanel.add(relatePanel, BorderLayout.CENTER);
    relateTabPanel.add(btnPanel, BorderLayout.NORTH);
    btnPanel.add(btnRunTests, null);
  }

  private void updateTestCaseIndexLabel() {
    testCaseIndexLabel.setText(AppStrings.LABEL_TEST_CASE + " " + currentTestCaseIndex + " of " + maxTestCaseIndex);
  }

  public double getStretchSize() {
    return ((Integer) spStretchDist.getValue()).intValue();
  }
}

