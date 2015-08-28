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

import junit.framework.TestCase;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SchemaMapperTest extends TestCase {

    final String[] columns1 = new String[]{"c1", "c2"};
    final String[] columns2 = new String[]{"c2", "c1"};
    final int[] metaTypes1 = new int[]{ValueMeta.TYPE_INTEGER, ValueMeta.TYPE_STRING};
    final int[] metaTypes2 = new int[]{ValueMeta.TYPE_STRING, ValueMeta.TYPE_NUMBER};
    final int[] metaTypesTarget = new int[]{ValueMeta.TYPE_STRING, ValueMeta.TYPE_STRING};
    final Object[] data1 = new Object[]{1, "hov"};
    final Object[] data2 = new Object[]{"guava", 1.5};

    private StepMeta createStreamSchemaStep(String name, PluginRegistry registry, String[] inputSteps, List<StepMeta> stepMetaList) {
        StreamSchemaStepMeta streamSchemaMeta = new StreamSchemaStepMeta();
        streamSchemaMeta.setStepsToMerge(inputSteps);
        for ( int i = 0; i < inputSteps.length; i++ ) {
            streamSchemaMeta.getStepIOMeta().addStream(
                    new Stream( StreamInterface.StreamType.INFO, null, "Streams to Merge", StreamIcon.INFO, inputSteps[i] ) );
        }
        streamSchemaMeta.searchInfoAndTargetSteps(stepMetaList);


        String uniqueListPid = registry.getPluginId(StepPluginType.class, streamSchemaMeta);

        return new StepMeta(uniqueListPid, name, streamSchemaMeta);
    }

    private void updateInfoStreams(StreamSchemaStepMeta sm, int nrsteps, String[] inputSteps) {

    }

    /**
     * Creates a row meta interface for the fields that are defined
     * @param valuesMetas defined ValueMetaInterface
     * @return RowMetaInterface
     */
    private RowMetaInterface createRowMetaInterface(ValueMetaInterface[] valuesMetas) {
        RowMetaInterface rm = new RowMeta();

        for (ValueMetaInterface aValuesMeta : valuesMetas) {
            rm.addValueMeta(aValuesMeta);
        }

        return rm;
    }

    private ValueMetaInterface[] genValueMetaArray(String[] columns, int[] metaTypes) {
        ValueMetaInterface[] result = new ValueMetaInterface[columns.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = new ValueMeta(columns[i], metaTypes[i]);
        }
        return result;
    }


    /**
     * Create input data for test case 1
     * @return list of metadata/data couples
     */
    private List<RowMetaAndData> createInputData(boolean isFirst) {
        List<RowMetaAndData> list = new ArrayList<RowMetaAndData>();
        String[] columns;
        int[] metaTypes;
        Object[] r1;
        if (isFirst) {
            columns = columns1;
            metaTypes = metaTypes1;
//            r1 = new Object[columns1.length];
//            System.arraycopy(data1, 0, r1, 0, columns1.length);
            r1 = data1;
        } else {
            columns = columns2;
            metaTypes = metaTypes2;
            r1 = data2;
        }
        ValueMetaInterface[] valuesMetas = genValueMetaArray(columns, metaTypes);
        RowMetaInterface rm = createRowMetaInterface(valuesMetas);

        list.add(new RowMetaAndData(rm , r1));

        return list;
    }

    /**
     * Create result data for test case 1. Each list object should mirror the output of the parsed JSON
     *
     * @return list of metadata/data couples of how the result should look.
     */
    private List<RowMetaAndData> createExpectedResults() {
        List<RowMetaAndData> list = new ArrayList<RowMetaAndData>();
        ValueMetaInterface[] valuesMetas = genValueMetaArray(columns1, metaTypesTarget);
        RowMetaInterface rm = createRowMetaInterface(valuesMetas);

        Object[] r1 = new Object[]{1, "hov"};
        Object[] r2 = new Object[]{1.5, "guava"};

        list.add(new RowMetaAndData(rm, r1));
        list.add(new RowMetaAndData(rm, r2));

        return list;
    }

    /**
     * Runs the transformation with the below input parameters
     * @return Transformation Results
     */
    private List<RowMetaAndData> test()
            throws Exception {
        KettleEnvironment.init();

        // Create a new transformation
        TransMeta transMeta = new TransMeta();
        transMeta.setName("testStreamSchemaMerge");
        PluginRegistry registry = PluginRegistry.getInstance();

        // Create Injector1
        String injectorStepName1 = "injector step 1";
        StepMeta injectorStep1 = TestUtilities.createInjectorStep(injectorStepName1, registry);
        transMeta.addStep(injectorStep1);

        // Create Injector2
        String injectorStepName2 = "injector step 2";
        StepMeta injectorStep2 = TestUtilities.createInjectorStep(injectorStepName2, registry);
        transMeta.addStep(injectorStep2);

        // Create a Stream Schema Merge step
        String streamSchemaStepName = "Stream Schema step";
        StepMeta streamSchemaStep = createStreamSchemaStep(streamSchemaStepName, registry,
                new String[]{injectorStepName1, injectorStepName2}, Arrays.asList(injectorStep1, injectorStep2));
        transMeta.addStep(streamSchemaStep);

        // TransHopMetas between injector steps and StreamSchema Step
        TransHopMeta injector1_hop = new TransHopMeta(injectorStep1, streamSchemaStep);
        transMeta.addTransHop(injector1_hop);
        TransHopMeta injector2_hop = new TransHopMeta(injectorStep2, streamSchemaStep);
        transMeta.addTransHop(injector2_hop);


        // Create a dummy step
        String dummyStepName = "dummy step";
        StepMeta dummyStep = TestUtilities.createDummyStep(dummyStepName, registry);
        transMeta.addStep(dummyStep);

        // TransHopMeta between StreamSchema and Dummy
        TransHopMeta streamSchema_hop_dummy = new TransHopMeta(streamSchemaStep, dummyStep);
        transMeta.addTransHop(streamSchema_hop_dummy);


        // Execute the transformation
        Trans trans = new Trans(transMeta);
        trans.prepareExecution(null);

        // Create a row collector and add it to the dummy step interface
        StepInterface si = trans.getStepInterface(dummyStepName, 0);
        RowStepCollector dummyRowCollector = new RowStepCollector();
        si.addRowListener(dummyRowCollector);

        // Create row producers
        RowProducer rowProducer1 = trans.addRowProducer(injectorStepName1, 0);
        RowProducer rowProducer2 = trans.addRowProducer(injectorStepName2, 0);
        trans.startThreads();

        // create the rows
        List<RowMetaAndData> inputList1 = createInputData(true);
        List<RowMetaAndData> inputList2 = createInputData(false);
        for (RowMetaAndData rowMetaAndData : inputList1) {
            rowProducer1.putRow(rowMetaAndData.getRowMeta(), rowMetaAndData.getData());
        }
        rowProducer1.finished();
        for (RowMetaAndData rowMetaAndData : inputList2) {
            rowProducer2.putRow(rowMetaAndData.getRowMeta(), rowMetaAndData.getData());
        }
        rowProducer2.finished();

        trans.waitUntilFinished();

        return dummyRowCollector.getRowsWritten();
    }

    public void testStreamSchema1() throws Exception {
        List<RowMetaAndData> transformationResults = test();
        List<RowMetaAndData> expectedResults = createExpectedResults();
        try {
            TestUtilities.checkRows(transformationResults, expectedResults, 0);
            checkValues(transformationResults, expectedResults);
        } catch(TestFailedException e) {
            fail(e.getMessage());
        }
    }

    /**
     * Helper method to check that the values of the fields match
     * @param transResults output from transformation
     * @param expectedResults expected output
     * @throws TestFailedException If values don't match
     */
    private void checkValues(List<RowMetaAndData> transResults, List<RowMetaAndData> expectedResults)
            throws TestFailedException {
        TestUtilities.checkRows(transResults, expectedResults, 0);
        Iterator<RowMetaAndData> itrRows1 = transResults.iterator();
        Iterator<RowMetaAndData> itrRows2 = expectedResults.iterator();
        while ( itrRows1.hasNext() && itrRows2.hasNext() ) {
            RowMetaAndData rowMetaAndData1 = itrRows1.next();
            RowMetaAndData rowMetaAndData2 = itrRows2.next();

            Object[] rowObject1 = rowMetaAndData1.getData();
            Object[] rowObject2 = rowMetaAndData2.getData();

            for (int i = 0; i < rowObject2.length; i++) {
                Object s1 = rowObject1[i];
                Object s2 = rowObject2[i];
                ValueMetaInterface vm = rowMetaAndData2.getValueMeta(i);
                try {
                    if (vm.compare(s1, s2) != 0) {
                        throw new TestFailedException("");
                    }
                } catch (Exception e) {
                    throw new TestFailedException(String.format("Values don't match. Found: %s Expected: %s", s1, s2));
                }
            }
        }
    }
}
