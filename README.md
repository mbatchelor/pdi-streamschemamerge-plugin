# Stream Schema Merge - PDI Plugin

[![Build Status](https://travis-ci.org/graphiq-data/pdi-streamschemamerge-plugin.svg)](https://travis-ci.org/graphiq-data/pdi-streamschemamerge-plugin)

This step allows you to merge streams with different schemas without first rearranging the fields, adding constants or modifying data meta types.

## Use Cases
Let's say you want to take 2 text files and combine them into 1 text file. 

### Simple Case
+ *Scenario:* The files have the exact same structure and field ordered
+ *Traditional Solution:* Simple transformation that reads in text files and sends rows directly to output

### Complex (Real-Word) Case
+ *Scenario:* The files share 6/10 fields and the other 4 fields are different in each file. The order of column names is not the same in the two files.
+ *Traditional Solution:* Use the Add Constants step to add missing fields, then use the Select Values step to rearrange the fields so they appear in the same order.

### Why Stream Schema Merge is Better
+ While the complex case is a little clunky, it isn't so bad with only 2 streams; however, it rapidly becomes messy and hard to implement as the number of streams increases. It doesn't scale.
+ The Stream Schema Merge step takes care of finding the union of all the fields you pass to it and placing the data values in the appropriate columns. Why use your brain for something a computer can do better? For example,
```
Stream 1 Fields: apple, orange
Stream 2 Fields: orange, banana
Stream 3 Fields: cherry, mango
Result Stream: `apple, orange, banana, cherry, mango`
```
If a stream did not originally contain a column (e.g. mango isn't in stream 1), then the value of that field will be null for all rows originating from that stream.

### Usage Notes
+ This step is not compatible with the "enable safe mode" option that is available when running transformations through spoon. This option will be automatically disabled when you run a transformation containing this step
+ If 2 streams contain the same field, but they have different data meta types, the data meta type of the field in the resulting stream will be String

## Installation

Easiest way is to download the binary release.

- Fetch the latest release from here: https://github.com/Pentaho-SE-EMEA-APAC/pdi-streamschemamerge-plugin/releases 
- Unzip the zip file
- Take the 'pdi-streamschema-merge' folder and drop in to PENTAHO_HOME/design-tools/data-integration/plugins
- Restart PDI/Spoon
- You will find the Stream Schema Merge step under the 'Flow' folder

## Samples

Sample transforms are available in the ZIP release, within the samples folder.

## Authors
+ [Andrew Overton](https://team.graphiq.com/l/232/Andrew-Overton) - aoverton at graphiq dot com (Original author)
+ [Adam Fowler](https://github.com/adamfowleruk) - adam dot fowler at hitachi vantara dot com (Updated for PDI 8.0 and Maven)

## License and Copyright

All material in this repository are Copyright 2002-2018 Hitachi Vantara. All code is licensed as Apache 2.0 unless explicitly stated. See the LICENSE file for more details.

## Support Statement

This work is at Stage 1 : Development Phase: Start-up phase of an internal project. Usually a Labs experiment. (Unsupported)