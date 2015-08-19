package com.graphiq.kettle.steps.streamschemamerge;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

/**
 * Takes in RowMetas and find the union of them. Then maps the field of each row to its final destination
 */
public class SchemaMapper {
    RowMetaInterface row;  // resolved row meta
    LinkedHashMap<Integer, HashMap<Integer, Integer>> mapping;

    public SchemaMapper(RowMetaInterface info[]) {
        unionMerge(info);
    }

    /**
     * Given RowMetas find the union of all of them. Create a mapping along the way so we know how to move the fields
     * into their appropriate place
     * @param info row metas for the fields to merge
     */
    private void unionMerge(RowMetaInterface info[]) {
        // do set up
        mapping = new LinkedHashMap<Integer, HashMap<Integer, Integer>>(info.length);
        RowMetaInterface base = info[0].clone();
        HashSet<String> fieldNames = new HashSet<String>();
        Collections.addAll(fieldNames, base.getFieldNames());

        // do merge
        for (int i = 0; i < info.length; i++) {
            HashMap<Integer, Integer> rowMapping = new HashMap<Integer, Integer>(info[i].size(), 1);
            int size = info[i].size();
            for (int x = 0; x < size; x++) {
                ValueMetaInterface field = info[i].getValueMeta(x);
                String name = field.getName();
                if (!fieldNames.contains(name)) {
                    base.addValueMeta(field);
                    fieldNames.add(name);
                }
                rowMapping.put(x, base.indexOfValue(name));  // update mapping for this field
            }
            mapping.put(i, rowMapping);  // save the mapping for this rowMeta
        }
        row = base;  // set our master output row
    }

    /**
     * Get mappings for all rows
     * @return mappings from all input rows to the output row format
     */
    public LinkedHashMap<Integer, HashMap<Integer, Integer>> getMapping() {
        return mapping;
    }

    /**
     * Get master output row
     * @return row meta for union of all output rows
     */
    public RowMetaInterface getRowMeta() {
        return row;
    }

}
