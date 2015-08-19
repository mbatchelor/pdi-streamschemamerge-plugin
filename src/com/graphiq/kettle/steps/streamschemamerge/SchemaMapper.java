package com.graphiq.kettle.steps.streamschemamerge;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.*;

/**
 * Takes in RowMetas and find the union of them. Then maps the field of each row to its final destination
 */
public class SchemaMapper {
    RowMetaInterface row;  // resolved row meta
    int[][] mapping;

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
        mapping = new int[info.length][];
        RowMetaInterface base = info[0].clone();
        HashSet<String> fieldNames = new HashSet<String>();
        Collections.addAll(fieldNames, base.getFieldNames());

        // do merge
        for (int i = 0; i < info.length; i++) {
            int[] rowMapping = new int[info[i].size()];
            for (int x = 0; x < rowMapping.length; x++) {
                ValueMetaInterface field = info[i].getValueMeta(x);
                String name = field.getName();
                if (!fieldNames.contains(name)) {
                    base.addValueMeta(field);
                    fieldNames.add(name);
                }
                rowMapping[x] = base.indexOfValue(name);  // update mapping for this field
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

}
