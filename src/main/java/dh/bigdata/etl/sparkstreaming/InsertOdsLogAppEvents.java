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

public class InsertOdsLogAppEvents implements Function2<JavaRDD<Row>, Time, Void> {

    private Broadcast<HiveContext> broadcastHC;

    public InsertOdsLogAppEvents(Broadcast<HiveContext> broadcastHC) {
        this.broadcastHC = broadcastHC;
    }

    @Override
    public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
        if (!rdd.isEmpty()) {
            List<StructField> structFields = new ArrayList<>();
            structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("vt", DataTypes.IntegerType, true));
            structFields.add(DataTypes.createStructField("imei", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("guid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("currevent", DataTypes.LongType, true));
            structFields.add(DataTypes.createStructField("isfirst", DataTypes.LongType, true));
            structFields.add(DataTypes.createStructField("eventtime", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("duration", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("defineid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pageid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("prepageid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sessionid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("userid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("eventid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pagedur", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("moduleid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("modulecnt", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("firstvt", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lastvt", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("tpvn", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("svum", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("tclk", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sclk", DataTypes.StringType, true));
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
            structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);

            HiveContext hiveContext = broadcastHC.value();
            DataFrame df = hiveContext.createDataFrame(rdd, structType);
            df.registerTempTable("tmp_ods_log_app_events");

            hiveContext.sql("set hive.exec.dynamic.partition=true");
            hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            hiveContext.sql("INSERT INTO sparkstreaming.tmp_ods_log_app_events PARTITION (dt) select * from tmp_ods_log_app_events");
        }
        return null;
    }
}
