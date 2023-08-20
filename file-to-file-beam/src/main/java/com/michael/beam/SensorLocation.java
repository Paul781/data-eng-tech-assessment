package com.michael.beam;

import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaFieldSchema.class)
class Location {
    public Double lon;
    public Double lat;

    @SchemaCreate
    public Location(Double lon, Double lat) {
        this.lon = lon;
        this.lat = lat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Location location = (Location) o;
        return Objects.equals(lon, location.lon) && Objects.equals(lat, location.lat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lon, lat);
    }
}

@DefaultSchema(JavaFieldSchema.class)
public class SensorLocation {
    public int location_id;
    public String sensor_description;
    public String sensor_name;
    @Nullable
    public String installation_date;
    @Nullable
    public String note;
    public String location_type;
    public String status;
    @Nullable
    public String direction_1;
    @Nullable
    public String direction_2;
    public Double latitude;
    public Double longitude;
    public Location location;

    @SchemaCreate
    public SensorLocation(int location_id,
                          String sensor_description,
                          String sensor_name,
                          String installation_date,
                          String note,
                          String location_type,
                          String status,
                          String direction_1,
                          String direction_2,
                          Double latitude,
                          Double longitude, Location location) {
        this.location_id = location_id;
        this.sensor_description = sensor_description;
        this.sensor_name = sensor_name;
        this.installation_date = installation_date;
        this.note = note;
        this.location_type = location_type;
        this.status = status;
        this.direction_1 = direction_1;
        this.direction_2 = direction_2;
        this.latitude = latitude;
        this.longitude = longitude;
        this.location = location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SensorLocation that = (SensorLocation) o;
        return location_id == that.location_id && sensor_description.equals(that.sensor_description) && sensor_name.equals(that.sensor_name)
                && Objects.equals(installation_date, that.installation_date) && Objects.equals(note, that.note)
                && Objects.equals(location_type, that.location_type) && Objects.equals(status, that.status)
                && Objects.equals(direction_1, that.direction_1) && Objects.equals(direction_2, that.direction_2)
                && Objects.equals(latitude, that.latitude) && Objects.equals(longitude, that.longitude) && location.equals(
                that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location_id, sensor_description, sensor_name, installation_date, note, location_type, status, direction_1,
                direction_2, latitude, longitude, location);
    }
}