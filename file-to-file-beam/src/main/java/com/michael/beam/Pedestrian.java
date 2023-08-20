package com.michael.beam;

import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaFieldSchema.class)
public class Pedestrian{
    public String timestamp;
    public String locationid;
    @Nullable
    public int direction_1;
    @Nullable
    public int direction_2;
    @Nullable
    public int total_of_directions;

    @SchemaCreate
    public Pedestrian(String timestamp,
                      String locationid,
                      int direction_1,
                      int direction_2,
                      int total_of_directions) {
        this.timestamp = timestamp;
        this.locationid = locationid;
        this.direction_1 = direction_1;
        this.direction_2 = direction_2;
        this.total_of_directions = total_of_directions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Pedestrian that = (Pedestrian) o;
        return direction_1 == that.direction_1 && direction_2 == that.direction_2 && total_of_directions == that.total_of_directions
                && timestamp.equals(that.timestamp) && locationid.equals(that.locationid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, locationid, direction_1, direction_2, total_of_directions);
    }
}