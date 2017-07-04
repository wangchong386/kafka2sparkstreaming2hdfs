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

public class InsertOdsLogAppEventsIos implements Function2<JavaRDD<Row>, Time, Void> {

    private Broadcast<HiveContext> broadcastHC;

    public InsertOdsLogAppEventsIos(Broadcast<HiveContext> broadcastHC) {
        this.broadcastHC = broadcastHC;
    }

    @Override
    public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
        if (!rdd.isEmpty()) {
            List<StructField> structFields = new ArrayList<>();
            structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("vt", DataTypes.IntegerType, true));
            structFields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("usrid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
            structFields.add(DataTypes.createStructField("ua", DataTypes.LongType, true));
            structFields.add(DataTypes.createStructField("aid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("u", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("refurl", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("platform", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("an", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("eventtime", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("dur", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("defineid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("eventname", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("firstvt", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lastvt", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("tpvn", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("svum", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("tclk", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sclk", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("appver", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("activityid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pushid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("d1coce", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("itemcode", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ptype", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("coupon", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("rfxno", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("schkw", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("f", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);

            HiveContext hiveContext = broadcastHC.value();
            DataFrame df = hiveContext.createDataFrame(rdd, structType);
            df.registerTempTable("tmp_ods_log_app_events_ios");

            hiveContext.sql("set hive.exec.dynamic.partition=true");
            hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            hiveContext.sql("INSERT INTO sparkstreaming.tmp_ods_log_app_events_ios PARTITION (dt) select * from tmp_ods_log_app_events_ios");
        }
        return null;
    }
}
