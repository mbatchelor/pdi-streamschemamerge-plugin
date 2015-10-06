/*! ******************************************************************************
*
* Pentaho Data Integration
*
* Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package com.graphiq.kettle.steps.streamschemamerge;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Button;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;

import java.util.List;


public class StreamSchemaStepDialog extends BaseStepDialog implements StepDialogInterface {

	private static Class<?> PKG = StreamSchemaStepMeta.class; // for i18n purposes

	// this is the object the stores the step's settings
	// the dialog reads the settings from it when opening
	// the dialog writes the settings to it when confirmed 
	private StreamSchemaStepMeta meta;

	private String[] previousSteps;  // steps sending data in to this step

	// text field holding the name of the field to add to the row stream
	private Label wlSteps;
	private TableView wSteps;
	private FormData fdlSteps, fdSteps;

	/**
	 * The constructor should simply invoke super() and save the incoming meta
	 * object to a local variable, so it can conveniently read and write settings
	 * from/to it.
	 * 
	 * @param parent 	the SWT shell to open the dialog in
	 * @param in		the meta object holding the step's settings
	 * @param transMeta	transformation description
	 * @param sname		the step name
	 */
	public StreamSchemaStepDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		meta = (StreamSchemaStepMeta) in;
	}

	/**
	 * This method is called by Spoon when the user opens the settings dialog of the step.
	 * It should open the dialog and return only once the dialog has been closed by the user.
	 * 
	 * If the user confirms the dialog, the meta object (passed in the constructor) must
	 * be updated to reflect the new step settings. The changed flag of the meta object must 
	 * reflect whether the step configuration was changed by the dialog.
	 * 
	 * If the user cancels the dialog, the meta object must not be updated, and its changed flag
	 * must remain unaltered.
	 * 
	 * The open() method must return the name of the step after the user has confirmed the dialog,
	 * or null if the user cancelled the dialog.
	 */
	public String open() {

		// store some convenient SWT variables 
		Shell parent = getParent();
		Display display = parent.getDisplay();

		// SWT code for preparing the dialog
		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, meta);
		
		// Save the value of the changed flag on the meta object. If the user cancels
		// the dialog, it will be restored to this saved value.
		// The "changed" variable is inherited from BaseStepDialog
		changed = meta.hasChanged();
		
		// The ModifyListener used on all controls. It will update the meta object to 
		// indicate that changes are being made.
		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				meta.setChanged();
			}
		};
		
		// ------------------------------------------------------- //
		// SWT code for building the actual settings dialog        //
		// ------------------------------------------------------- //
		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "StreamSchemaStep.Shell.Title"));

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName")); 
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

        // OK, get and cancel buttons
        wOK = new Button( shell, SWT.PUSH );
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wGet = new Button( shell, SWT.PUSH );
        wGet.setText( BaseMessages.getString( PKG, "StreamSchema.getPreviousSteps.Label" ) );
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{wOK, wGet, wCancel}, margin, null);

		// Table with fields for inputting step names
		wlSteps = new Label( shell, SWT.NONE );
		wlSteps.setText(BaseMessages.getString(PKG, "StreamSchemaStepDialog.Steps.Label"));
		props.setLook(wlSteps);
		fdlSteps = new FormData();
		fdlSteps.left = new FormAttachment( 0, 0 );
		fdlSteps.top = new FormAttachment( wStepname, margin );
		wlSteps.setLayoutData(fdlSteps);

		final int FieldsCols = 1;
        final int FieldsRows = meta.getNumberOfSteps();

        previousSteps = transMeta.getPrevStepNames(stepname);

		ColumnInfo[] colinf = new ColumnInfo[FieldsCols];
		colinf[0] =
				new ColumnInfo(
						BaseMessages.getString( PKG, "StreamSchemaStepDialog.StepName.Column" ),
						ColumnInfo.COLUMN_TYPE_CCOMBO, previousSteps, false );

		wSteps =
				new TableView(
						transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

		fdSteps = new FormData();
		fdSteps.left = new FormAttachment( 0, 0 );
		fdSteps.top = new FormAttachment(wlSteps, margin );
		fdSteps.right = new FormAttachment( 100, 0 );
		fdSteps.bottom = new FormAttachment( wOK, -2 * margin );
		wSteps.setLayoutData(fdSteps);

		// Add listeners for cancel and OK
		lsCancel = new Listener() {
			public void handleEvent(Event e) {cancel();}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {ok();}
		};
        lsGet = new Listener() {
            public void handleEvent( Event e ) {
                get();
            }
        };

		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);
        wGet.addListener( SWT.Selection, lsGet );

		// default listener (for hitting "enter")
		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {ok();}
		};
		wStepname.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {cancel();}
		});
		
		// Set/Restore the dialog size based on last position on screen
		// The setSize() method is inherited from BaseStepDialog
		setSize();

		// populate the dialog with the values from the meta object
		populateDialog();
		
		// restore the changed flag to original value, as the modify listeners fire during dialog population 
		meta.setChanged(changed);

		// open dialog and enter event loop 
		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}

		// at this point the dialog has closed, so either ok() or cancel() have been executed
		// The "stepname" variable is inherited from BaseStepDialog
		return stepname;
	}
	
	/**
	 * This helper method puts the step configuration stored in the meta object
	 * and puts it into the dialog controls.
	 */
    private void populateDialog() {
        Table table = wSteps.table;
        if ( meta.getNumberOfSteps() > 0 ) {
            table.removeAll();
        }
        String[] stepNames = meta.getStepsToMerge();
        for ( int i = 0; i < stepNames.length; i++ ) {
            TableItem ti = new TableItem( table, SWT.NONE );
            ti.setText( 0, "" + ( i + 1 ) );
            if ( stepNames[i] != null ) {
                ti.setText( 1, stepNames[i] );
            }
        }

        wSteps.removeEmptyRows();
        wSteps.setRowNums();
        wSteps.optWidth(true);

        wStepname.selectAll();
        wStepname.setFocus();
	}

    /**
     * Populates the table with a list of fields that have incoming hops
     */
    private void get() {
        wSteps.removeAll();
        Table table = wSteps.table;

        for ( int i = 0; i < previousSteps.length; i++ ) {
            TableItem ti = new TableItem( table, SWT.NONE );
            ti.setText( 0, "" + ( i + 1 ) );
            ti.setText( 1, previousSteps[i] );
        }
        wSteps.removeEmptyRows();
        wSteps.setRowNums();
        wSteps.optWidth(true);

    }

	/**
	 * Called when the user cancels the dialog.  
	 */
	private void cancel() {
		// The "stepname" variable will be the return value for the open() method. 
		// Setting to null to indicate that dialog was cancelled.
		stepname = null;
		// Restoring original "changed" flag on the met aobject
		meta.setChanged(changed);
		// close the SWT dialog window
		dispose();
	}

    /**
     * Helping method to update meta information when ok is selected
     * @param inputSteps Names of the steps that are being merged together
     */
    private void getMeta(String[] inputSteps) {
        List<StreamInterface> infoStreams = meta.getStepIOMeta().getInfoStreams();

        if ( infoStreams.size() == 0 || inputSteps.length < infoStreams.size()) {
            if ( inputSteps.length != 0 ) {
                meta.wipeStepIoMeta();
                for (String inputStep : inputSteps) {
                    meta.getStepIOMeta().addStream(
                            new Stream(StreamInterface.StreamType.INFO, null, "", StreamIcon.INFO, null));
                }
                infoStreams = meta.getStepIOMeta().getInfoStreams();
            }
        } else if ( infoStreams.size() < inputSteps.length ) {
            int requiredStreams = inputSteps.length - infoStreams.size();

            for ( int i = 0; i < requiredStreams; i++ ) {
                meta.getStepIOMeta().addStream(
                        new Stream( StreamInterface.StreamType.INFO, null, "", StreamIcon.INFO, null ) );
            }
            infoStreams = meta.getStepIOMeta().getInfoStreams();
        }
        int streamCount = infoStreams.size();

        String[] stepsToMerge = meta.getStepsToMerge();
        for ( int i = 0; i < streamCount; i++ ) {
            String step = stepsToMerge[i];
            StreamInterface infoStream = infoStreams.get( i );
            infoStream.setStepMeta( transMeta.findStep( step ) );
            infoStream.setSubject(step);
        }
    }

    @Override
    protected Button createHelpButton(Shell shell, StepMeta stepMeta, PluginInterface plugin) {
        plugin.setDocumentationUrl("https://github.com/graphiq-data/pdi-streamschemamerge-plugin/blob/master/help.md");
        return super.createHelpButton(shell, stepMeta, plugin);
    }
	
	/**
	 * Called when the user confirms the dialog
	 */
	private void ok() {
		// The "stepname" variable will be the return value for the open() method. 
		// Setting to step name from the dialog control
		stepname = wStepname.getText(); 
		// set output field name

        // TODO eliminate copying here and copying when placed in meta
        int nrsteps = wSteps.nrNonEmpty();
        String[] stepNames = new String[nrsteps];
        for ( int i = 0; i < nrsteps; i++ ) {
            TableItem ti = wSteps.getNonEmpty(i);
            StepMeta tm = transMeta.findStep(ti.getText(1));
            if (tm != null) {
                stepNames[i] = tm.getName();
            }
        }
        meta.setStepsToMerge(stepNames);
		getMeta(stepNames);

		// close the SWT dialog window
		dispose();
	}
}
