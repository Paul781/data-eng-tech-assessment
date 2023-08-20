package com.michael.beam;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

public class SideinputPipelineTest {

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    // Create initial data for test
//    static final String testPedestrianData =
//            "{\n"
//                    + "        \"timestamp\": \"2023-04-20T16:00:00+00:00\",\n"
//                    + "        \"locationid\": \"1\",\n"
//                    + "        \"direction_1\": 2,\n"
//                    + "        \"direction_2\": 19,\n"
//                    + "        \"total_of_directions\": 21\n"
//                    + "    },{\n"
//
//                    + "        \"timestamp\": \"2023-04-20T20:00:00+00:00\",\n"
//                    + "        \"locationid\": \"1\",\n"
//                    + "        \"direction_1\": 25,\n"
//                    + "        \"direction_2\": 45,\n"
//                    + "        \"total_of_directions\": 70\n"
//                    + "    }";
    static final List<String> testPedestrianData = Arrays.asList(
            "{\n"
                    + "        \"timestamp\": \"2023-04-20T16:00:00+00:00\",\n"
                    + "        \"locationid\": \"1\",\n"
                    + "        \"direction_1\": 2,\n"
                    + "        \"direction_2\": 19,\n"
                    + "        \"total_of_directions\": 21\n"
                    + "    }",
            "    {\n"
                    + "        \"timestamp\": \"2023-04-20T20:00:00+00:00\",\n"
                    + "        \"locationid\": \"1\",\n"
                    + "        \"direction_1\": 25,\n"
                    + "        \"direction_2\": 45,\n"
                    + "        \"total_of_directions\": 70\n"
                    + "    }"
    );

    static final String testLocationData =
            "  {\n"
                    + "    \"location_id\": 1,\n"
                    + "    \"sensor_description\": \"Bourke Street Mall (North)\",\n"
                    + "    \"sensor_name\": \"Bou292_T\",\n"
                    + "    \"installation_date\": \"2009-03-24\",\n"
                    + "    \"note\": null,\n"
                    + "    \"location_type\": \"Outdoor\",\n"
                    + "    \"status\": \"A\",\n"
                    + "    \"direction_1\": \"East\",\n"
                    + "    \"direction_2\": \"West\",\n"
                    + "    \"latitude\": -37.81349441,\n"
                    + "    \"longitude\": 144.96515323,\n"
                    + "    \"location\": {\n"
                    + "      \"lon\": 144.96515323,\n"
                    + "      \"lat\": -37.81349441\n"
                    + "    }"
                    + "    }";




    @Test
    public void testEnrichedData() throws NoSuchSchemaException {

        // Mock the inputs
        PCollection<String> mockedLocationData = pipeline.apply("create location data",Create.of(testLocationData));
        PCollection<String> mockedPedestrianData = pipeline.apply("create pedestraian data",Create.of(testPedestrianData));


        final String[] expectedOutput = new String[] {
                "{\"timestamp\":\"2023-04-20T16:00:00+00:00\","
                        + "\"locationid\":\"1\","
                        + "\"direction_1\":2,"
                        + "\"direction_2\":19,"
                        + "\"total_of_directions\":21,"
                        + "\"sensorDescription\":\"Bourke Street Mall (North)\"}",
                "{\"timestamp\":\"2023-04-20T20:00:00+00:00\","
                        + "\"locationid\":\"1\","
                        + "\"direction_1\":25,"
                        + "\"direction_2\":45,"
                        + "\"total_of_directions\":70,"
                        + "\"sensorDescription\":\"Bourke Street Mall (North)\"}"
        };


        PCollection<KV<Integer, String>> locationMap = mockedLocationData.apply("parse location", JsonToRow.withSchema(pipeline.getSchemaRegistry().getSchema(SensorLocation.class)))
                .apply("convert location",Convert.fromRows(SensorLocation.class))
                .apply("create location map",ParDo.of(new SideinputPipeline.ParseLocationFn()));



        final PCollectionView<Map<Integer, String>> locationMapView = locationMap.apply("create side input view",View.asMap());


        PCollection<String> enrichedData = mockedPedestrianData.apply("parse pedestrian", JsonToRow.withSchema(pipeline.getSchemaRegistry().getSchema(
                Pedestrian.class)))
                .apply("convert pedestrain",Convert.fromRows(Pedestrian.class))
                .apply("enrich",ParDo.of(new SideinputPipeline.EnrichFn(locationMapView))
                        .withSideInputs(locationMapView));




        // Assert
        PAssert.that(enrichedData)
                .containsInAnyOrder(expectedOutput);

        // Run
        pipeline.run().waitUntilFinish();
    }
}
