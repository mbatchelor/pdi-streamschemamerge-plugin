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
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

	public String[] stepNames;  // rowset names for incoming rowsets

	public RowSet r;  // used for iterating over rowsets
}
	
