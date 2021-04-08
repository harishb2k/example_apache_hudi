package com.devlibx.data.source;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;
import org.joda.time.DateTime;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DataGenerator {

    private <T> GenericRecord pojoToRecord(T model) throws Exception {
        Schema schema = ReflectData.get().getSchema(model.getClass());
        ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        datumWriter.write(model, encoder);
        encoder.flush();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
        return datumReader.read(null, decoder);
    }

    public List<HoodieRecord> generateHoodieRecord(int from, int to) {
        return convert(generateData(from, to));
    }

    public List<HoodieRecord> convert(List<User> users) {
        return users.stream().map(user -> {
            try {
                GenericRecord genericRecord = pojoToRecord(user);
                HoodieRecordPayload payload = new OverwriteWithLatestAvroPayload(Option.of(genericRecord));
                HoodieKey key = new HoodieKey(user.getUserId(), "");
                return new HoodieRecord(key, payload);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                return null;
            }
        }).collect(Collectors.toList());
    }

    public List<User> generateData(int from, int to) {
        List<User> users = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            List<Ride> rides = new ArrayList<>();
            for (int j = 0; j <= 100; j++) {
                int id = i * 10_000 + j;
                Ride ride = Ride.builder()
                        .id("" + id)
                        .drop("drop_" + id)
                        .pickup("pickup_" + id)
                        .price(10.0)
                        .time(DateTime.now().toString())
                        .build();

                if (i == 30) {
                    // TODO - Make changes to user 30
                    // ride.setDrop("updated_1_30_" + id);
                }
                rides.add(ride);
            }

            User user = User.builder()
                    .userId(i + "")
                    .city("city_" + (i % 10))
                    .age(10)
                    .rides(rides)
                    .build();
            users.add(user);
        }
        return users;
    }
}
