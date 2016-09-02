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

import org.apache.commons.vfs.FileObject;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

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
        dis.setSafeModeEnabled(false);  // safe mode is incompatible with this step
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
		data.rowSets = new ArrayList<RowSet>();
        data.stepNames = new ArrayList<String>(data.numSteps);
		data.inputRowSetNumbers = new LinkedList<Integer>();
		data.files = new LinkedList<FileObject>();
		data.outStreams = new ArrayList<ObjectOutputStream>(data.infoStreams.size());
		data.inStreams = new ArrayList<ObjectInputStream>(data.infoStreams.size());
		data.inputRowSetNames = new LinkedList<String>();
		data.cacheRowMetaMap = new HashMap<Integer, RowMetaInterface>();
		data.cacheRowSetNameMap = new HashMap<Integer, String>();
		for (int i = 0; i < data.infoStreams.size(); i++) {
			try {
				FileObject fileObject = KettleVFS.createTempFile("streamschema", ".tmp", System.getProperty("java.io.tmpdir"), getTransMeta());
				data.files.add(fileObject);
				ObjectOutputStream outputStream = new ObjectOutputStream(KettleVFS.getOutputStream(fileObject, false));
				data.outStreams.add(outputStream);
			} catch (Exception e) {
				logError("Unable to create file object");
				return false;
			}
		}

		return super.init(meta, data);
	}

	private static int findInfoStreamPosition(List<StreamInterface> infoStreams, String stepName) {
		for (int i = 0; i < infoStreams.size(); i++) {
			if (infoStreams.get(i).getStepname().equals(stepName)) {
				return i;
			}
		}
		return -1;
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
            data.foundARowMeta = false;
            for (int i = 0; i < data.infoStreams.size(); i++) {
				data.r = findInputRowSet(data.infoStreams.get(i).getStepname());
                if (data.r == null) {
					// see if the rowset has been removed by the calls to getRow below that prevent the step from blocking
					// in which case we should have stored the relevant info for retrieval
					if (data.cacheRowMetaMap.containsKey(i)) {
						data.rowMetas[i] = data.cacheRowMetaMap.get(i);
						data.stepNames.add(data.cacheRowSetNameMap.get(i));
						data.foundARowMeta = true;
						continue;
					}
					throw new KettleException(String.format("Missing a rowset for %s", data.infoStreams.get(i).getStepname()));
				}
				data.rowSets.add(data.r);
				data.stepNames.add(data.r.getName());
                // Avoids race condition. Row metas are not available until the previous steps have called
                // putRowWait at least once
                data.iterations = 0;
				data.completedLoopedPostDoneSignal = false;
				data.doneSignal = false;
				while (data.rowMetas[i] == null && !data.completedLoopedPostDoneSignal && !isStopped()) {
                    data.rowMetas[i] = data.r.getRowMeta();
                    data.iterations++;
                    if (data.doneSignal) {
                        // we have completed a loop after the done signal
                        data.completedLoopedPostDoneSignal = true;
                    }
                    if (data.r.isDone()) {
                        // we've received the done signal
                        data.doneSignal = true;
                    }
					/*
					 This step blocks until it gets data from all input row sets (or the rowsets say they're done)
					 This means that you can encounter issues if you split a stream with a filter, do some action
					 and then join it back together with this step and one of the steps hasn't received any steps before the blocking starts.
					 You can get deadlocked. This alleviates the issue by freeing room in the blocking rowset and
					 storing it on disk.
					 */
					if (data.iterations > data.ACCUMULATION_TRIGGER) {
						try {
							Object [] row = getRow();
							if (row != null) {
								/*
								Calling getRow can wind up removing the rowSet from the list of rowsets.
								Therefore, we need to store the RowMeta and RowSet Name for the rowsets as they
								correspond to the infostreams as we get them via getRow. The RowMeta and RowSetName
								are the ultimate reason we're doing everything up until this point.
								We need the RowMetas to build the Schema Mapper and we need the RowSet Names's to do
								lookups later on to determine what mapping scheme to use
								 */
								int rowSetNum = getCurrentInputRowSetNr();
								RowSet curr = getInputRowSets().get(rowSetNum);
								// todo investigate treatment of step copies
								int infostreamNum = findInfoStreamPosition(data.infoStreams, curr.getOriginStepName());
								if (infostreamNum < 0) {
									throw new KettleException("Unable to find a valid infostream");
								}
								data.cacheRowMetaMap.put(infostreamNum, curr.getRowMeta());
								data.cacheRowSetNameMap.put(infostreamNum, curr.getName());

								/*
								we need to store row set numbers and row set name for each row so we can reference
								this info when we pull these rows off to process later
								 */
								data.outStreams.get(rowSetNum).writeObject(row);
								data.inputRowSetNumbers.add(rowSetNum);
								data.inputRowSetNames.add(curr.getName());
							} else {
								logDebug(String.format("Found null at %d", data.inputRowSetNumbers.size()));
							}
						} catch (IOException e) {
							throw new KettleException(e.getMessage());
						}
					}
                }

                if (data.rowMetas[i] != null) {
					data.foundARowMeta = true;  // indicates at least one rowset is sending rows
				} else {
					logBasic(String.format("No meta found for %d", i));
				}
                if (isDebug()) {
                    logDebug("Iterations: " + data.iterations);
                }
			}

			// close output streams and open input streams
			try {
				for (ObjectOutputStream os: data.outStreams) {
					os.close();
				}
				for (int index = 0; index < data.files.size(); index++) {
					data.inStreams.add(new ObjectInputStream(KettleVFS.getInputStream(data.files.get(index))));
				}
				logDebug("Buffered rows: " + data.inputRowSetNumbers.size());
			} catch (IOException ex) {
				throw new KettleException("Error closing outstreams and opening in streams: " + ex.getMessage());
			}

            if (!data.foundARowMeta) {
                // none of the steps are sending rows so indicate we're done
                setOutputDone();
                return false;
            }

			// set up are mapping structures
			data.schemaMapping = new SchemaMapper(data.rowMetas);  // creates mapping and master output row
			data.mapping = data.schemaMapping.getMapping();
			data.outputRowMeta = data.schemaMapping.getRowMeta();
            data.convertToString = data.schemaMapping.getConvertToString();
			setInputRowSets(data.rowSets);  // set the order of the inputrowsets to match the order we've defined
            if (isDetailed()) {
                logDetailed("Finished generating mapping");
            }

		}

		Object[] incomingRow;
		if (data.inputRowSetNumbers.size() > 0) {
			// clear cache before reading rows form rowset again
			try {
				incomingRow = (Object []) data.inStreams.get(data.inputRowSetNumbers.remove()).readObject();
			} catch (Exception e) {
				throw new KettleException("Error reading buffered rows: " + e.getMessage());
			}
		} else {
			incomingRow = getRow();  // get the next available row
		}

		// if no more rows are expected, indicate step is finished and processRow() should not be called again
		if (incomingRow == null){
			setOutputDone();
			return false;
		}

		if (data.inputRowSetNames.size() > 0) {
			// we're reading from the cache not the rowset
			data.currentName = data.inputRowSetNames.remove();
		} else {
			// get the name of the step that the current rowset is coming from
			data.currentName = getInputRowSets().get(getCurrentInputRowSetNr()).getName();
		}
        // because rowsets are removed from the list of rowsets once they're exhausted (in the getRow() method) we
        // need to use the name to find the proper index for our lookups later
		data.streamNum = -1;
		for (int i = 0; i < data.stepNames.size(); i++) {
			if (data.stepNames.get(i) != null && data.stepNames.get(i).equals(data.currentName)) {
				data.streamNum = i;
				break;
			}
		}
		if (data.streamNum < 0) {
			throw new KettleException(String.format("Failed to find a matching step name for %s in the stepnames",
					data.currentName));
		}
        if (isRowLevel()) {
            logRowlevel(String.format("Current row from %s. This maps to stream number %d", data.currentName,
                    data.streamNum));
        }

        // create a new (empty) output row in the model of the master outputer row
		Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

		data.rowMapping = data.mapping[data.streamNum];  // set appropriate row mapping
		data.inRowMeta = data.rowMetas[data.streamNum];  // set appropriate meta for incoming row
		for (int j = 0; j < data.inRowMeta.size(); j++) {
            int newPos = data.rowMapping[j];
            // map a fields old position to its new position
            if (data.convertToString.contains(newPos) && incomingRow[j] != null) {
                // we need to convert the underlying data type to string
                outputRow[newPos] = incomingRow[j].toString();
            } else {
                outputRow[newPos] = incomingRow[j];
            }

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
        data.mapping = null;
        data.currentName = null;
        data.rowMapping = null;
        data.stepNames = null;
        data.r = null;
		data.inputRowSetNumbers = null;
		data.outStreams = null;
		for (ObjectInputStream is: data.inStreams) {
			try {
				is.close();
			} catch (IOException e) {
				logBasic("Hit exception when cleaning up: " + e.getMessage());
			}
		}
		data.outStreams = null;
		data.files = null;


        super.dispose(meta, data);
    }

}
