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

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds data objects used in StreamSchemaStep
 */
public class StreamSchemaStepData extends BaseStepData implements StepDataInterface {

	public RowMetaInterface outputRowMeta, inRowMeta;  // outgoing and incoming row meta

	public StreamSchemaStepData()
	{
		super();
	}

	public SchemaMapper schemaMapping;  // object that does row mapping

	public List<StreamInterface> infoStreams;  // streams of the incoming steps

	public List<RowSet> rowSets;  // a list of rowsets that are sending data to this step

	public RowMetaInterface[] rowMetas;  // a list of row meta information for incoming rows

	public int[][] mapping;  // mappings for all incoming rows

	public int numSteps, streamNum;  // incoming steps and what stream the current row is from

	public String currentName;  // name of the rowset that sent the current row

	public int[] rowMapping;  // row mapping for the current row

	public List<String> stepNames;  // rowset names for incoming rowsets

	public RowSet r;  // used for iterating over rowsets

    public boolean foundARowMeta;  // indicates that rows are being sent to the step

    public int iterations;  // used to track how many loops have occurred looking for rowsets

    public Set<Integer> convertToString; // used when we have to resolve data type mismatches

	public BufferedOutputStream inputRowSetNumbersOut;  // row set numbers for the rows written to disk
	public BufferedInputStream inputRowSetNumbersIn;  // row set numbers for the rows written to disk

	public FileObject inputRowSetFileObj;

	public ObjectOutputStream inputRowSetNamesOut;  // row set names for rows written to disk
	public ObjectInputStream inputRowSetNamesIn;  // row set names for rows written to disk

	public FileObject inputRowSetNamesFileObj;

	public boolean closedCacheFiles; // used to see if we've closed inputRowSetNumbersOut and inputRowSetNamesOut

	public long numBufferedRows;

	public int ACCUMULATION_TRIGGER = 100000;  // the number of iterations before we decide to start writing rows to disk (to prevent blocking)

	public List<ObjectOutputStream> outStreams;  // streams for writing cached rows

	public List<ObjectInputStream> inStreams;  // streams for reading cached rows

	public List<FileObject> files;

	public Map<Integer, RowMetaInterface> cacheRowMetaMap;  // lookup for row metas that might get removed when writing rows to disk

	public Map<Integer, String> cacheRowSetNameMap;  // lookup for row set names that might get removed when writing rows to disk

	public boolean completedLoopedPostDoneSignal = false;  // this ensures that we run 1 final time after the done signal

	public boolean doneSignal = false;  // we can have an infinite loop if a step isn't sending any rows

	public int remainingRowSetRetries = 10;

	public final int BUFFER_SIZE = 8388608;

}
	
