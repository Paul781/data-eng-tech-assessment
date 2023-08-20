package com.michael.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.PipelineOptions;

import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.util.Map;



public class SideinputPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(SideinputPipeline.class);
    private static final Gson gson = new Gson();

    public interface Options extends PipelineOptions {
        @Description("Path of the file to read from")
        @Validation.Required
        String getInput();

        void setInput(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }


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

   static class EnrichFn extends DoFn<Pedestrian, String> {

       private final PCollectionView<Map<Integer, String>> locView;

       public EnrichFn(PCollectionView<Map<Integer, String>> locView) {
           this.locView = locView;
       }

       @ProcessElement
       public void processElement(@Element Pedestrian pedestrian, OutputReceiver<String> out, ProcessContext c) {
           Map<Integer, String> locationInfo = c.sideInput(locView);
           // Enrich the data using the locationMapView
           try {
               String sensor_description = locationInfo.get(Integer.valueOf(pedestrian.locationid));

               if (sensor_description != null) {

                   JsonElement jsonElement = gson.toJsonTree(pedestrian);
                   jsonElement.getAsJsonObject().addProperty("sensorDescription", sensor_description);
                   String jsonStr = gson.toJson(jsonElement);
                   // Enrich other fields as needed
                   out.output(jsonStr);
               }
           } catch (Exception e) {
               LOG.error("Error enrich pedestrian data: ", e);
               out.output("Error for record: " + pedestrian.timestamp + ":" + pedestrian.locationid + ", Reason: " + e.getMessage());
           }
       }
   }

    public static void main(String[] args) throws NoSuchSchemaException{

        final TupleTag<String> successTag = new TupleTag<String>(){};
        final TupleTag<String> errorTag = new TupleTag<String>(){};

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(Options.class);

        Pipeline p = Pipeline.create(options);

        // Read the location data into a PCollection
        PCollection<String> locationData = p.apply(
                "Read Location Data", TextIO.read().from(options.getInput()+"/pedestrian-counting-system-sensor-locations.json"))
//                .withDelimiter(new byte[] {'\n'}
                .apply("parse JSON array",ParDo.of(new ParseJsonFn()));


        // Convert location data to a map for efficient lookup
        PCollection<KV<Integer, String>> locationMap = null;
            JsonToRow.ParseResult parseResult = locationData.apply("Parse location Json file", JsonToRow.withExceptionReporting(p.getSchemaRegistry().getSchema(SensorLocation.class)).withExtendedErrorInfo());
            parseResult.getFailedToParseLines().setRowSchema(JsonToRow.JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA)
                    .apply("invalida json object", ParDo.of(new LogErrorForJsonParseFn()));

            locationMap= parseResult.getResults().apply(Convert.fromRows(SensorLocation.class))
//                    ParseJsons.of(SensorLocation.class)).setCoder(SerializableCoder.of(SensorLocation.class))
            .apply("Convert to Location Map", ParDo.of(new ParseLocationFn()));



        final PCollectionView<Map<Integer, String>> locationMapView = locationMap.apply(View.asMap());

        // Enrich pedestrian data with the Location data as side inputs
        PCollection<String> pedestrianData = p.apply(
                "Read Pedestrian Data", TextIO.read().from(options.getInput()+"/pedestrian-counting-system-monthly-counts-per-hour.json"))
                        .apply("parse JSON array",ParDo.of(new ParseJsonFn()));

        PCollectionTuple enrichedData = null;
            enrichedData = pedestrianData.apply("Parse pedestrian Json file", JsonToRow.withSchema(p.getSchemaRegistry().getSchema(
                    Pedestrian.class)))
                    .apply(Convert.fromRows(Pedestrian.class))
                    .apply("Enrich Data", ParDo.of(new EnrichFn(locationMapView)).withOutputTags(successTag, TupleTagList.of(errorTag))
                    .withSideInputs(locationMapView));

        // Write successful records to desired output
        enrichedData.get(successTag)
                .apply("Write Enriched Data", TextIO.write().to(options.getOutput()));
        //.withNumShards(...)

        // Write error records to an error file
        enrichedData.get(errorTag)
                .apply("Write Error Records", TextIO.write().to(options.getOutput() + ".errors"));



        p.run().waitUntilFinish();
}

}