package dh.bigdata.etl.sparkstreaming;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Time;

import java.util.ArrayList;
import java.util.List;

public class InsertOdsLogAppDevice implements Function2<JavaRDD<Row>, Time, Void> {

    private Broadcast<HiveContext> broadcastHC;

    public InsertOdsLogAppDevice(Broadcast<HiveContext> broadcastHC) {
        this.broadcastHC = broadcastHC;
    }

    @Override
    public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
        if (!rdd.isEmpty()) {
            List<StructField> structFields = new ArrayList<>();
            structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sessionid", DataTypes.IntegerType, true));
            structFields.add(DataTypes.createStructField("vt", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("appid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("appver", DataTypes.LongType, true));
            structFields.add(DataTypes.createStructField("channel", DataTypes.LongType, true));
            structFields.add(DataTypes.createStructField("sdkver", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("userid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("guid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("imei", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("module", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("screen", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("density", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("mac", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("udid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("advertid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("vendorid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ismobile", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("isp", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("nettype", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("netstatus", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sim", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("tel", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("longitude", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("latitude", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("locprov", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("platform", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("os", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("iscrack", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ln", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("timezone", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("requesttime", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);

            HiveContext hiveContext = broadcastHC.value();
            DataFrame df = hiveContext.createDataFrame(rdd, structType);
            df.registerTempTable("tmp_ods_log_app_device");

            hiveContext.sql("set hive.exec.dynamic.partition=true");
            hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            hiveContext.sql("INSERT INTO sparkstreaming.tmp_ods_log_app_device PARTITION (dt) select * from tmp_ods_log_app_device");
        }
        return null;
    }
}
