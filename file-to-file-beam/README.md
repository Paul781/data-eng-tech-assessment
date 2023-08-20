### Dependencies on Mac:
* Maven (`brew install maven`)
* Java 15

### Command Detail:
* Check output
    * `cat tmp/output*`
* Run unit test by maven
  * `make test`
* Create jar file
  * `make jar`
* Run beam App by direct runner. This main class `SideinputPipeline` will use side input.
  * `make run-local-sideinput`

* Run beam App by direct runner. This main class `CoGroupByKeyPipeline` will use CoGroupByKey.
  * `make run-local-cogroupbykey`

* Run beam App by Flink runner. This main class will use side input.
  * `make run-flink-sideinput`



### Note:
- Split large JSON files into smaller chunks for efficient parallel processing.
  - chunk your JSON data into smaller files. Once chunked, you can then read each file in parallel with Beam by wildcard file pattern and flinkRunner. The Runner determines how to distribute the reading of these files across workers. If you're using a distributed runner and have multiple files, each worker can read a different file in parallel.
  - If the file is split into multiple parts (which is the case in distributed storage like GCS, S3, etc.), Beam will process the parts in parallel.
  - When you work with distributed storage systems like Google Cloud Storage, Amazon S3, or HDFS, Beam can take advantage of the distributed nature of these systems to read from multiple file parts or shards concurrently.
  - Make sure that the runner you're using supports parallelism and that you've properly configured the parallelism settings (like the number of workers) for optimal performance.
  - Beam's TextIO.read() will automatically read it line-by-line for one single file




- If the pedestrian-counting-system-sensor-locations.json file is too large, consider using a join operation, not sideinput.
  - When you use a join operation in a distributed processing system, the system can split, distribute, and process the data across multiple nodes in parallel  
  - Join Operations: Apache Beam provides join transforms like CoGroupByKey that you can use to join two datasets.
  - when you load a large dataset as a side input, it's broadcasted to each worker node, which can be inefficient 


- Use the right coder or create the custom coder to enhance the performance not using the default SerializableCoder which is inefficient and non-deterministic.
  - Use `JsonToRow` transform to convert JSON strings is better than `ParseJsons`. A PCollection with a schema does not need to have a Coder specified, Beam uses a special cider (SchemaCoder) to encode schema types, which is more efficient than SerializableCoder.
  - A schema on a PCollection enables a rich variety of relational transforms, similar to the aggregations in a SQL expression.


- Combine is a Beam transform for combining collections of elements or values in your data. which can be used to merge all the json string as one single json array from all collections.


- `JsonToRow` transform can not deal with the JSON Array with "[" and "]" in the beginning and end

### Limitations:
- The data in output file should have the same order as the input file (counts-per-hour file)?
- The format of output file does not matter? must be Json file?
- If the json string in the input file does not in the single line like multiple lines, how to handle it?
- Not only enrich the message by the location name,but also can do something like two direction names with different counts in the same location, how to update the code to implement it? 



```
incomming object-> minio -> event notification -> kafka -> flink -> minio
                                                            ^
                                                            |
                                                          minio
```





