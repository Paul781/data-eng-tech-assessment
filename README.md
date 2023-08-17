## Task

This is a timeboxed task that requires you to develop a set of datapipelines / tasks that process pedestrian data form the city of melbourne.
This task will require you to:

1. Create a beam that loads an external street data file (from the pedestrian-counting-system-monthly-counts-per-hour.json)
2. Read each record and enrich the message with the true location names (from the pedestrian-counting-system-sensor-locations.json file)
3. Finally write the enriched data into a new file
4. Have at least one unit test
5. Instructions on how to build you pipelines

### Further information

- The language preference is Java and then other JVM
- Don't over engineer, keep to the brief (there's plenty of room within that)

### Getting started

- Fork this repo as a starting point
- unzip the pedestrian.zip file in source-data

### Submission

Once you have completed you solution share the github repo with your HR contact with instructions on how to build your jars, do not share prebuilt jar files.

### Task files
1. Melbourne pedestrian data - newer version or different format can be downloaded [Here](https://discover.data.vic.gov.au/dataset/pedestrian-counting-system-monthly-counts-per-hour)
2. Location data - newer version or different format can be downloaded [Here](https://discover.data.vic.gov.au/dataset/pedestrian-counting-system-sensor-locations)

## Points of interest
- performance
- scale
- memory usage
- errors and validation


