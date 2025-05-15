import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;

import java.io.IOException;

public class AthensAirQualityIngestor {

    private static final String HBASE_TABLE_NAME = "air_quality";
    private static final String CF_GEO = "geo";
    private static final String CF_DATETIME = "datetime";
    private static final String CF_POLLUTION = "pollution";
    private static final String CF_WEATHER = "weather";

    public static void main(String[] args) throws IOException {
        ingestAirQualityData();
    }

    public static void ingestAirQualityData() throws IOException {

        SparkSession spark = SparkSession.builder()
                .appName("AthensAirQualityIngestion")
                .getOrCreate();

        Configuration hbaseConfig = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(HBASE_TABLE_NAME);

            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_GEO));
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_DATETIME));
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_POLLUTION));
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_WEATHER));

            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("HBase table created: " + HBASE_TABLE_NAME);
        }

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("hdfs:///data/athens_batch_data.csv");

        df.printSchema();
        df.show(5);
        long rowCount = df.count();
        System.out.println("Total rows to process: " + rowCount);

        df.foreachPartition((ForeachPartitionFunction<Row>) partition -> {
            Configuration conf = HBaseConfiguration.create();

            try (Connection connection = ConnectionFactory.createConnection(conf);
                 BufferedMutator mutator = connection.getBufferedMutator(
                         new BufferedMutatorParams(TableName.valueOf(HBASE_TABLE_NAME))
                                 .writeBufferSize(5 * 1024 * 1024))) {

                int rowsProcessed = 0;

                while (partition.hasNext()) {
                    Row row = partition.next();
                    String stationName = getStringSafe(row, "station_name");
                    String date = getStringSafe(row, "Date");

                    if (stationName == null || date == null) {
                        System.out.println("Warning: Skipping row due to missing key fields");
                        continue;
                    }

                    String rowKey = stationName + "_" + date;
                    Put put = new Put(Bytes.toBytes(rowKey));

                    safeAddColumn(put, row, CF_GEO, "lat", "Latitude");
                    safeAddColumn(put, row, CF_GEO, "long", "Longitude");
                    safeAddColumn(put, row, CF_GEO, "station_name", "station_name");
                    safeAddColumn(put, row, CF_GEO, "code", "code");
                    safeAddColumn(put, row, CF_GEO, "id", "id");

                    safeAddColumn(put, row, CF_DATETIME, "date", "Date");

                    safeAddColumn(put, row, CF_POLLUTION, "PM10", "PM10");
                    safeAddColumn(put, row, CF_POLLUTION, "PM2.5", "PM2.5");
                    safeAddColumn(put, row, CF_POLLUTION, "NO2", "NO2");
                    safeAddColumn(put, row, CF_POLLUTION, "O3", "O3");

                    safeAddColumn(put, row, CF_WEATHER, "temp", "Temp");
                    safeAddColumn(put, row, CF_WEATHER, "dewpoint", "Dewpoint Temp");
                    safeAddColumn(put, row, CF_WEATHER, "humidity", "Relative Humidity");
                    safeAddColumn(put, row, CF_WEATHER, "wind_u", "Wind-Speed (U)");
                    safeAddColumn(put, row, CF_WEATHER, "wind_v", "Wind-Speed (V)");
                    safeAddColumn(put, row, CF_WEATHER, "soil_temp", "Soil Temp");
                    safeAddColumn(put, row, CF_WEATHER, "precipitation", "Total Percipitation");
                    safeAddColumn(put, row, CF_WEATHER, "veg_high", "Vegitation (High)");
                    safeAddColumn(put, row, CF_WEATHER, "veg_low", "Vegitation (Low)");

                    mutator.mutate(put);
                    rowsProcessed++;

                    if (rowsProcessed % 1000 == 0) {
                        mutator.flush();
                        System.out.println("Processed " + rowsProcessed + " rows in this partition");
                    }
                }

                mutator.flush();
                System.out.println("Completed processing " + rowsProcessed + " rows in this partition");

            } catch (Exception e) {
                System.err.println("Error in partition processing: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Failed to process partition", e);
            }
        });

        System.out.println("Data ingestion completed!");
        spark.stop();
    }

    private static void safeAddColumn(Put put, Row row, String cf, String colName, String fieldName) {
        try {
            String value = getStringSafe(row, fieldName);
            if (value != null) {
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(value));
            }
        } catch (Exception e) {
            System.err.println("Error adding column " + cf + ":" + colName + " - " + e.getMessage());
        }
    }

    private static String getStringSafe(Row row, String fieldName) {
        try {
            if (row == null || fieldName == null) return null;

            String[] fieldNames = row.schema().fieldNames();
            for (String field : fieldNames) {
                if (field.equals(fieldName)) {
                    Object val = row.getAs(fieldName);
                    return val != null ? val.toString() : null;
                }
            }
            return null;
        } catch (Exception e) {
            System.err.println("Error getting field " + fieldName + " - " + e.getMessage());
            return null;
        }
    }
}
