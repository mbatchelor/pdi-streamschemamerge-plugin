package com.graphiq.kettle.steps.streamschemamerge;

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.pentaho.di.core.row.ValueMetaInterface.TYPE_STRING;



/**
 * Takes in RowMetas and find the union of them. Then maps the field of each row to its final destination
 */
public class SchemaMapper {
    RowMetaInterface row;  // resolved row meta
    int[][] mapping;
    Set<Integer> convertToString = new HashSet<Integer>();

    public SchemaMapper(RowMetaInterface info[]) throws KettlePluginException {
        unionMerge(info);
    }

    /**
     * Given RowMetas find the union of all of them. Create a mapping along the way so we know how to move the fields
     * into their appropriate place
     * @param info row metas for the fields to merge
     */
    private void unionMerge(RowMetaInterface info[]) throws KettlePluginException {
        // do set up
        mapping = new int[info.length][];
        RowMetaInterface base = null;
        for (int i = 0; i < info.length; i++) {
            if (info[i] != null) {  // handles cases where some steps don't send any rows
                base = info[i].clone();
                break;
            }
        }
        HashSet<String> fieldNames = new HashSet<String>();
        Collections.addAll(fieldNames, base.getFieldNames());

        // do merge
        for (int i = 0; i < info.length; i++) {
            int[] rowMapping = null;
            if (info[i] != null) {  // handles cases where some steps don't send any rows
                rowMapping = new int[info[i].size()];
                for (int x = 0; x < rowMapping.length; x++) {
                    ValueMetaInterface field = info[i].getValueMeta(x);
                    String name = field.getName();
                    if (!fieldNames.contains(name)) {
                        base.addValueMeta(field);
                        fieldNames.add(name);
                    }
                    int basePosition = base.indexOfValue(name);
                    rowMapping[x] = basePosition;  // update mapping for this field
                    // check if we need to change the data type to string
                    ValueMetaInterface baseField = base.getValueMeta(basePosition);
                    if (baseField.getType() != field.getType()) {
                        ValueMetaInterface updatedField = ValueMetaFactory.cloneValueMeta(baseField, TYPE_STRING);
                        base.setValueMeta(basePosition, updatedField);
                        convertToString.add(basePosition);  // we need to change the data type of these fields
                    }
                }
            }
            mapping[i] = rowMapping;  // save the mapping for this rowMeta
        }
        row = base;  // set our master output row
    }

    /**
     * Get mappings for all rows
     * @return mappings from all input rows to the output row format
     */
    public int[][] getMapping() {
        return mapping;
    }

    /**
     * Get master output row
     * @return row meta for union of all output rows
     */
    public RowMetaInterface getRowMeta() {
        return row;
    }

    /**
     * Return set of fields that need to be converted to strings
     * @return set of field positions that need to be converted to strings
     */
    public Set<Integer> getConvertToString() {
        return convertToString;
    }
}
