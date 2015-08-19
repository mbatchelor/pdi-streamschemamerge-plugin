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

import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Merge streams from multiple different steps into a single stream. Unlike most other steps, this step does NOT
 * require the incoming rows to have the same RowMeta. Instead, this step will examine the incoming rows and take the
 * union of the set of all rows passed in. Fields that have the same name will be placed in the same field. The field
 * type will be taken from the first occurrence of a field.
 *
 * Because this step combines multiple streams with different RowMetas together, it is deemed "not safe" and will fail
 * if you try to run the transformation with the "Enable Safe Mode checked". Therefore it disables safe mode
 *
 * @author aoverton
 * @since 18-aug-2015
 *
 */

public class StreamSchemaStep extends BaseStep implements StepInterface {

	/**
	 * The constructor should simply pass on its arguments to the parent class.
	 *
	 * @param s 				step description
	 * @param stepDataInterface	step data class
	 * @param c					step copy
	 * @param t					transformation description
	 * @param dis				transformation executing
	 */
	public StreamSchemaStep(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
        super(s, stepDataInterface, c, t, dis);
        dis.setSafeModeEnabled(false);  // safe mode is incomapitable with this step
	}

	/**
	 * Initialize data structures that we have information for at init time
	 *
	 * @param smi 	step meta interface implementation, containing the step settings
	 * @param sdi	step data interface implementation, used to store runtime information
	 *
	 * @return true if initialization completed successfully, false if there was an error
	 *
	 */
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		// Casting to step-specific implementation classes is safe
		StreamSchemaStepMeta meta = (StreamSchemaStepMeta) smi;
		StreamSchemaStepData data = (StreamSchemaStepData) sdi;

		data.infoStreams = meta.getStepIOMeta().getInfoStreams();
		data.numSteps = data.infoStreams.size();
		data.rowMetas = new RowMetaInterface[data.numSteps];
		data.rowMetaList = new ArrayList<RowMetaInterface>();
		data.rowSets = new ArrayList<RowSet>();
        data.stepNames = new String[data.numSteps];

		return super.init(meta, data);
	}

	/**
	 * For each row, create a new output row in the model of the master output row and copy the data values in to the
     * appropriate indexes
	 *
	 * @param smi the step meta interface containing the step settings
	 * @param sdi the step data interface that should be used to store
	 *
	 * @return true to indicate that the function should be called again, false if the step is done
	 */
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		StreamSchemaStepMeta meta = (StreamSchemaStepMeta) smi;
		StreamSchemaStepData data = (StreamSchemaStepData) sdi;

        /*
         * Code in first method is responsible for finishing the initialization that we couldn't do earlier
         */
		if (first) {
			first = false;

            for (int i = 0; i < data.infoStreams.size(); i++) {
                data.r = findInputRowSet(data.infoStreams.get(i).getStepname());
                data.rowSets.add(data.r);
                data.stepNames[i] = data.r.getName();
                // Avoids race condition. Row metas are not available until the previous steps have called
                // putRowWait at least once
                while (data.rowMetas[i] == null && !isStopped()) {
                    data.rowMetas[i] = data.r.getRowMeta();
                }
            }

			data.schemaMapping = new SchemaMapper(data.rowMetas);  // creates mapping and master output row
			data.mapping = data.schemaMapping.getMapping();
			data.outputRowMeta = data.schemaMapping.getRowMeta();
			Collections.addAll(data.rowMetaList, data.rowMetas);
			setInputRowSets(data.rowSets);  // set the order of the inputrowsets to match the order we've defined


		}

		Object[] incomingRow = getRow();  // get the next available row

		// if no more rows are expected, indicate step is finished and processRow() should not be called again
		if (incomingRow == null){
			setOutputDone();
			return false;
		}

        // get the name of the step that the current rowset is coming from
		data.currentName = getInputRowSets().get(getCurrentInputRowSetNr()).getName();
        // because rowsets are removed from the list of rowsets once they're exhausted (in the getRow() method) we
        // need to use the name to find the proper index for our lookups later
		for (int i = 0; i < data.stepNames.length; i++) {
			if (data.stepNames[i].equals(data.currentName)) {
				data.streamNum = i;
				break;
			}
		}

        // create a new (empty) output row in the model of the master outputer row
		Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

		data.rowMapping = data.mapping.get(data.streamNum);  // set appropriate row mapping
		data.inRowMeta = data.rowMetaList.get(data.streamNum);  // set appropriate meta for incoming row
		for (int j = 0; j < data.inRowMeta.size(); j++) {
            Integer newPos = data.rowMapping.get(j);
			outputRow[newPos] = incomingRow[j];  // map a fields old position to its new position
		}

		// put the row to the output row stream
		putRow(data.outputRowMeta, outputRow);

		// log progress if it is time to to so
		if (checkFeedback(getLinesRead())) {
			logBasic("Linenr " + getLinesRead()); // Some basic logging
		}

		// indicate that processRow() should be called again
		return true;
	}

    /**
     * Clear steps from step data
     * @param smi the step meta interface containing the step settings
     * @param sdi the step data interface that should be used to store
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {

        // Casting to step-specific implementation classes is safe
        StreamSchemaStepMeta meta = (StreamSchemaStepMeta) smi;
        StreamSchemaStepData data = (StreamSchemaStepData) sdi;

        data.outputRowMeta = null;
        data.inRowMeta = null;
        data.schemaMapping = null;
        data.infoStreams = null;
        data.rowSets = null;
        data.rowMetas = null;
        data.rowMetaList = null;
        data.mapping = null;
        data.currentName = null;
        data.rowMapping = null;
        data.stepNames = null;
        data.r = null;

        super.dispose(meta, data);
    }

}
