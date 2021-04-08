package org.example;

import com.devlibx.data.source.DataGenerator;
import com.devlibx.data.source.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.QuickstartUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {

        if (true) {
            Schema schema = ReflectData.get().getSchema(User.class);
            System.out.println(schema);
            // return;
        }

        String tableName = "user_data";
        String basePath = "file:///tmp/user_data/iteration_2/user_data";

        SparkSession spark = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.config("deploy-mode", "client")
                .getOrCreate();
        DataGenerator dataGenerator = new DataGenerator();
        List<HoodieRecord> data = dataGenerator.generateHoodieRecord(1, 100);

        List<User> dataUser = dataGenerator.generateData(1, 100);
        ObjectMapper objectMapper = new ObjectMapper();
        List<String> dataString = dataUser.stream().map(user -> {
            try {
                return objectMapper.writeValueAsString(user);
            } catch (JsonProcessingException e) {
                return null;
            }
        }).collect(Collectors.toList());


        //spark.createDataFrame(data, HoodieRecord.class)
        spark.read().json(spark.createDataset(dataString, Encoders.STRING()))
                .write()
                .format("hudi")
                .options(QuickstartUtils.getQuickstartWriteConfigs())
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "userId")
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "userId")
                .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "city")
                .option(HoodieWriteConfig.TABLE_NAME, tableName)
                .mode(SaveMode.Overwrite)
                .save(basePath);
    }
}
