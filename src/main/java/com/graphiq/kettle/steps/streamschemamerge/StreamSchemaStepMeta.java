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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;


@Step(	
		id = "StreamSchemaStep",
		image = "com/graphiq/kettle/steps/streamschemamerge/resources/icon.svg",
		i18nPackageName="com.graphiq.kettle.steps.streamschemamerge",
		name="StreamSchemaStep.Name",
		description = "StreamSchemaStep.TooltipDesc",
		categoryDescription="StreamSchemaStep.Category"
)
public class StreamSchemaStepMeta extends BaseStepMeta implements StepMetaInterface {

	private static Class<?> PKG = com.graphiq.kettle.steps.streamschemamerge.StreamSchemaStepMeta.class; // for i18n purposes

    /**
     * Stores the names of the steps to merge into the output
     */
    private ArrayList<String> stepsToMerge = new ArrayList<String>();

	/**
	 * Constructor should call super() to make sure the base class has a chance to initialize properly.
	 */
	public StreamSchemaStepMeta() {
		super(); 
	}

    /**
     * Prevents error box from popping up when sending in different row formats. Note you will still get an error if
     * you try to run the transformating in safe mode.
     * @return true
     */
    public boolean excludeFromRowLayoutVerification() {
        return true;
    }
	
	/**
	 * Called by Spoon to get a new instance of the SWT dialog for the step.
	 * A standard implementation passing the arguments to the constructor of the step dialog is recommended.
	 * 
	 * @param shell		an SWT Shell
	 * @param meta 		description of the step 
	 * @param transMeta	description of the the transformation 
	 * @param name		the name of the step
	 * @return 			new instance of a dialog for this step 
	 */
	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new StreamSchemaStepDialog(shell, meta, transMeta, name);
	}

	/**
	 * Called by PDI to get a new instance of the step implementation. 
	 * A standard implementation passing the arguments to the constructor of the step class is recommended.
	 * 
	 * @param stepMeta				description of the step
	 * @param stepDataInterface		instance of a step data class
	 * @param cnr					copy number
	 * @param transMeta				description of the transformation
	 * @param disp					runtime implementation of the transformation
	 * @return						the new instance of a step implementation 
	 */
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
		return new StreamSchemaStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
	}

	/**
	 * Called by PDI to get a new instance of the step data class.
	 */
	public StepDataInterface getStepData() {
		return new StreamSchemaStepData();
	}

	/**
	 * This method is called every time a new step is created and should allocate/set the step configuration
	 * to sensible defaults. The values set here will be used by Spoon when a new step is created.
	 */
	public void setDefault() {
		// intentionally empty
	}

    /**
     * Getter for the fields that should be merged
     * @return array of field names
     */
    public String[] getStepsToMerge() {
        if (stepsToMerge == null) {
            return new String[0];
        } else {
            return stepsToMerge.toArray(new String[stepsToMerge.size()]);
        }

    }

    /**
     * Determine the number of steps we're planning to merge
     * @return number of items to merge, 0 if none
     */
    public int getNumberOfSteps() {
        if (stepsToMerge == null) {
            return 0;
        } else {
            return stepsToMerge.size();
        }
    }

    /**
     * Set steps to merge
     * @param arrayOfSteps Names of steps to merge
     */
    public void setStepsToMerge(String[] arrayOfSteps) {
        stepsToMerge = new ArrayList<String>();
        Collections.addAll(stepsToMerge, arrayOfSteps);
    }

    /**
	 * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
	 * step meta object. Be sure to create proper deep copies if the step configuration is stored in
	 * modifiable objects.
	 * 
	 * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on creating
	 * a deep copy.
	 * 
	 * @return a deep copy of this
	 */
	public Object clone() {
		Object retval = super.clone();
		return retval;
	}
	
	/**
	 * This method is called by Spoon when a step needs to serialize its configuration to XML. The expected
	 * return value is an XML fragment consisting of one or more XML tags.  
	 * 
	 * Please use org.pentaho.di.core.xml.XMLHandler to conveniently generate the XML.
	 * 
	 * @return a string containing the XML serialization of this step
	 */
	public String getXML() throws KettleValueException {
		StringBuilder xml = new StringBuilder();
        xml.append( "    <steps>" + Const.CR );
        for (StreamInterface infoStream : getStepIOMeta().getInfoStreams()) {
            xml.append( "      <step>" + Const.CR );
            xml.append( "        " + XMLHandler.addTagValue( "name", infoStream.getStepname() ) );
            xml.append( "      </step>" + Const.CR );
        }
        xml.append("      </steps>" + Const.CR);
		return xml.toString();
	}

	/**
	 * This method is called by PDI when a step needs to load its configuration from XML.
	 * 
	 * Please use org.pentaho.di.core.xml.XMLHandler to conveniently read from the
	 * XML node passed in.
	 * 
	 * @param stepnode	the XML node containing the configuration
	 * @param databases	the databases available in the transformation
	 * @param metaStore the metaStore to optionally read from
	 */
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {

        readData(stepnode);

	}

    /**
     * Helper methods to read in the XML
     * @param stepnode XML node for the step
     * @throws KettleXMLException If there is an error reading the configuration
     */
    private void readData( Node stepnode) throws KettleXMLException {
        try {
            //TODO put the strings in a config file or make constants in this file
            Node steps = XMLHandler.getSubNode( stepnode, "steps" );
            int nrsteps = XMLHandler.countNodes( steps, "step" );

            stepsToMerge.clear();

            // we need to add a stream for each step we want to merge to ensure it gets treated as an info stream
            for ( int i = 0; i < nrsteps; i++ ) {
                getStepIOMeta().addStream(
                        new Stream( StreamInterface.StreamType.INFO, null, "Streams to Merge", StreamIcon.INFO, null ) );
            }

            List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
            for ( int i = 0; i < nrsteps; i++ ) {
                Node fnode = XMLHandler.getSubNodeByNr( steps, "step", i );
                String name = XMLHandler.getTagValue(fnode, "name");
                stepsToMerge.add(name);
                infoStreams.get(i).setSubject(name);
            }
        } catch ( Exception e ) {
            throw new KettleXMLException( "Unable to load step info from XML", e );
        }
    }

	/**
	 * This method is called by Spoon when a step needs to serialize its configuration to a repository.
	 * The repository implementation provides the necessary methods to save the step attributes.
	 *
	 * @param rep					the repository to save to
	 * @param metaStore				the metaStore to optionally write to
	 * @param id_transformation		the id to use for the transformation when saving
	 * @param id_step				the id to use for the step  when saving
	 */
	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException
	{
		try{
            for (int i = 0; i < stepsToMerge.size(); i++) {
                rep.saveJobEntryAttribute(id_transformation, id_step, i, stepsToMerge.get(i), "mergeStepName");
            }
		}
		catch(Exception e){
			throw new KettleException(BaseMessages.getString(PKG, "StreamSchemaStep.RepoSaveError")+id_step, e);
		}
	}		
	
	/**
	 * This method is called by PDI when a step needs to read its configuration from a repository.
	 * The repository implementation provides the necessary methods to read the step attributes.
	 * 
	 * @param rep		the repository to read from
	 * @param metaStore	the metaStore to optionally read from
	 * @param id_step	the id of the step being read
	 * @param databases	the databases available in the transformation
	 */
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException  {
		try{
            int nrSteps = rep.countNrStepAttributes( id_step, "mergeStepName" );
			for ( int i = 0; i < nrSteps; i++ ) {
				getStepIOMeta().addStream(
						new Stream( StreamInterface.StreamType.INFO, null, "Streams to Merge", StreamIcon.INFO, null ) );
			}
			List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
			for ( int i = 0; i < nrSteps; i++ ) {
				String name = rep.getStepAttributeString(id_step, i, "mergeStepName");
				stepsToMerge.add(name);
				infoStreams.get(i).setSubject(name);
			}
		}
		catch(Exception e){
			throw new KettleException(BaseMessages.getString(PKG, "StreamSchemaStep.RepoLoadError"), e);
		}
	}

	/**
	 * This method is called to determine the changes the step is making to the row-stream.
	 * To that end a RowMetaInterface object is passed in, containing the row-stream structure as it is when entering
	 * the step. This method must apply any changes the step makes to the row stream. Usually a step adds fields to the
	 * row-stream.
	 * 
	 * @param inputRowMeta		the row structure coming in to the step
	 * @param name 				the name of the step making the changes
	 * @param info				row structures of any info steps coming in
	 * @param nextStep			the description of a step this step is passing rows to
	 * @param space				the variable space for resolving variables
	 * @param repository		the repository instance optionally read from
	 * @param metaStore			the metaStore to optionally read from
	 */
	public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

		/*
		 * We don't have any input fields so we ingore inputRowMeta
		 */
        try {
            SchemaMapper schemaMapping = new SchemaMapper(info);  // compute the union of the info fields being passed in
            RowMetaInterface base = schemaMapping.getRowMeta();

            for ( int i = 0; i < base.size(); i++ ) {
                base.getValueMeta( i ).setOrigin( name );
            }
            inputRowMeta.mergeRowMeta(base);
        } catch (KettlePluginException e) {
            throw new KettleStepException("Kettle plugin exception trying to resolve fields");
        }

		
	}

	/**
	 * This method is called when the user selects the "Verify Transformation" option in Spoon. 
	 * A list of remarks is passed in that this method should add to. Each remark is a comment, warning, error, or ok.
	 * The method should perform as many checks as necessary to catch design-time errors.
	 * 
	 *   @param remarks		the list of remarks to append to
	 *   @param transMeta	the description of the transformation
	 *   @param stepMeta	the description of the step
	 *   @param prev		the structure of the incoming row-stream
	 *   @param input		names of steps sending input to the step
	 *   @param output		names of steps this step is sending output to
	 *   @param info		fields coming in from info steps 
	 *   @param metaStore	metaStore to optionally read from
	 */
	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info, VariableSpace space, Repository repository, IMetaStore metaStore)  {
		
		CheckResult cr;

		// See if there are input streams leading to this step!
		if (input.length > 0) {
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "StreamSchemaStep.CheckResult.ReceivingRows.OK"), stepMeta);
			remarks.add(cr);
		} else {
			cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "StreamSchemaStep.CheckResult.ReceivingRows.ERROR"), stepMeta);
			remarks.add(cr);
		}	
    	
	}

	@Override
	public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
		for ( StreamInterface stream : getStepIOMeta().getInfoStreams() ) {
			stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
		}
	}

    public void resetStepIoMeta() {
        // Do nothing, don't reset as there is no need to do this.
    }

    /**
     * Has original function of resetStepIoMeta, but we only want to call it when appropriate
     */
	public void wipeStepIoMeta() {
		ioMeta = null;
	}


}
