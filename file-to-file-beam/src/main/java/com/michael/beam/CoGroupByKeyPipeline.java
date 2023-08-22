package com.michael.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.PipelineOptions;

import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CoGroupByKeyPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(CoGroupByKeyPipeline.class);
    // reuse the gson to reduce the memory usage
    private static final Gson gson = new Gson();

    static class ParseJsonFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> out) {

            try {
                String jsonString = line;
                JsonArray jsonArray = JsonParser.parseString(jsonString).getAsJsonArray();
                for (JsonElement jsonObject : jsonArray) {
                    out.output(gson.toJson(jsonObject));
                }
            } catch (Exception e) {
                LOG.error("Error parsing json data: ", e);
            }
        }
    }

    static class LogErrorForJsonParseFn extends DoFn<Row, Void> {

        @ProcessElement
        public void process(@Element Row row) {
            LOG.error(row.toString());
        }
    }

    static class ParseLocationFn extends DoFn<SensorLocation, KV<Integer, String>> {
        @ProcessElement
        public void processElement(@Element SensorLocation location, OutputReceiver<KV<Integer, String>> out) {

            try {
                out.output(KV.of(location.location_id, location.sensor_description));
            } catch (Exception e) {
                LOG.error("Error parsing location data: ", e);
            }
        }
    }

    static class ParsePedestrianFn extends DoFn<Pedestrian, KV<Integer, Pedestrian>> {
        @ProcessElement
        public void processElement(@Element Pedestrian pedestrian, OutputReceiver<KV<Integer, Pedestrian>> out) {

            try {
                out.output(KV.of(Integer.valueOf(pedestrian.locationid), pedestrian));
            } catch (Exception e) {
                LOG.error("Error parsing Pedestrian data: ", e);
            }
        }
    }

    static class EnrichDataFn extends DoFn<KV<Integer, CoGbkResult>, String> {

        private final TupleTag<Pedestrian> pedestrianTag;
        private final TupleTag<String> locationTag;

        public EnrichDataFn(TupleTag<Pedestrian> pedestrianTag, TupleTag<String> locationTag) {
            this.pedestrianTag = pedestrianTag;
            this.locationTag = locationTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<Integer, CoGbkResult> e = c.element();
            Integer location_id = e.getKey();
            String location_description = e.getValue().getOnly(locationTag);
            Iterable<Pedestrian> pedestrianIter = e.getValue().getAll(pedestrianTag);

            for (Pedestrian pe : pedestrianIter) {
                JsonElement jsonElement = gson.toJsonTree(pe);
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                jsonObject.addProperty("sensorDescription", location_description);
                c.output(gson.toJson(jsonObject));
            }


        }
    }


    public interface Options extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("/tmp")
        String getInput();

        void setInput(String value);

        @Description("Path of the file to write to")
        @Default.String("/tmp/cogroup/output")
        String getOutput();

        void setOutput(String value);
    }

    public static void main(String[] args) throws NoSuchSchemaException{

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(Options.class);

        Pipeline p = Pipeline.create(options);


        final TupleTag<Pedestrian> pedestrianTag = new TupleTag<>();
        final TupleTag<String> locationTag = new TupleTag<>();

        // Read and parse sensor locations
        PCollection<String> locationData  = p
                    .apply("ReadLocations", TextIO.read().from(options.getInput()+"/pedestrian-counting-system-sensor-locations.json"))
                    .apply("parse JSON array for location",ParDo.of(new ParseJsonFn()));



        JsonToRow.ParseResult parseResult = locationData.apply("ParseLocations", JsonToRow.withExceptionReporting(p.getSchemaRegistry().getSchema(SensorLocation.class)).withExtendedErrorInfo());
        parseResult.getFailedToParseLines().setRowSchema(JsonToRow.JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA)
                .apply("invalida json object", ParDo.of(new LogErrorForJsonParseFn()));

        PCollection<SensorLocation> sensorLocations = parseResult.getResults().apply("Convert to location class", Convert.fromRows(SensorLocation.class));
        PCollection<KV<Integer, String>> locationMap = sensorLocations.apply("Convert to Location Map", ParDo.of(new ParseLocationFn()));

        // Read and parse pedestrian data
        PCollection<Pedestrian> pedestrians = p
                    .apply("ReadPedestrian", TextIO.read().from(options.getInput()+"/pedestrian-counting-system-monthly-counts-per-hour.json"))
                    .apply("parse JSON array for pedestrian",ParDo.of(new ParseJsonFn()))
                    .apply("ParsePedestrian", JsonToRow.withSchema(p.getSchemaRegistry().getSchema(Pedestrian.class)))
                    .apply("Convert to Pedestrian class",Convert.fromRows(Pedestrian.class));
        PCollection<KV<Integer, Pedestrian>> pedestrianMap = pedestrians.apply("Convert to pedestrians Map", ParDo.of(new ParsePedestrianFn()));


        // Perform the co-group to process data efficiently
        PCollection<KV<Integer, CoGbkResult>> results =
                KeyedPCollectionTuple.of(locationTag, locationMap)
                        .and(pedestrianTag, pedestrianMap)
                        .apply("co group by key",CoGroupByKey.create());

        PCollection<String> enrichedData =
                results.apply("enrich data",
                        ParDo.of( new EnrichDataFn(pedestrianTag,locationTag)));


        // Write the enriched data
        enrichedData.apply("Write Enriched Data",TextIO.write().to(options.getOutput()));
//        withoutSharding() & withNumShards(...)

        p.run().waitUntilFinish();
    }


}

