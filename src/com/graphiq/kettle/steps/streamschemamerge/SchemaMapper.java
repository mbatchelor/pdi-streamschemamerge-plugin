package com.graphiq.kettle.steps.streamschemamerge;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

/**
 * Created by aoverton on 8/12/15.
 */
public class SchemaMapper {
    /*
     * Instance Variables
     */

    RowMetaInterface row;  // resolved row meta
    LinkedHashMap<Integer, HashMap<Integer, Integer>> mapping;

    public enum SchemaMergeType {
        UNION, INTERSECT, CONFORM
    }


    /*
     * Constructors
     */

    public SchemaMapper(RowMetaInterface info[], SchemaMergeType type) {
        switch (type) {
            case UNION:
                unionMerge(info);
                break;
            default:
                throw new IllegalArgumentException("Incorrect SchemaMergeType must be from enum choices");
        }
    }

    /*
     * Methods
     */
    private void unionMerge(RowMetaInterface info[]) {
        // setup
        mapping = new LinkedHashMap<Integer, HashMap<Integer, Integer>>(info.length);  // default load factor of 1
        RowMetaInterface base = info[0].clone();  // base could be set in step
        HashSet<String> fieldNames = new HashSet<String>();  // might use searchValueMeta instead
        String[] fields = base.getFieldNames();
        Collections.addAll(fieldNames, fields);
        // merge
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
                rowMapping.put(x, base.indexOfValue(name));
            }
            mapping.put(i, rowMapping);
        }
        row = base.clone();
    }

    public LinkedHashMap<Integer, HashMap<Integer, Integer>> getMapping() {
        return mapping;
    }

    public RowMetaInterface getRow() {
        return row;
    }

}
