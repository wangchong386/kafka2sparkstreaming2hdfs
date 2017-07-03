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

public class InsertOdsLogPageView implements Function2<JavaRDD<Row>, Time, Void> {

    private Broadcast<HiveContext> broadcastHC;

    public InsertOdsLogPageView(Broadcast<HiveContext> broadcastHC) {
        this.broadcastHC = broadcastHC;
    }

    @Override
    public Void call(JavaRDD<Row> rdd, Time time) throws Exception {
        if (!rdd.isEmpty()) {
            List<StructField> structFields = new ArrayList<>();
            structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
            structFields.add(DataTypes.createStructField("vid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("usrid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("vt", DataTypes.LongType, true));
            structFields.add(DataTypes.createStructField("ip", DataTypes.LongType, true));
            structFields.add(DataTypes.createStructField("ua", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("f", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("d", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("refurl", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ulevel", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("aid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("u", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("cou", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("uuid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pt", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lpid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("activity", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("supplierid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pic", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pos", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("clkloc", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("catepubid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("picl", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pril", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pnl", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("spcst", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("wdc", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("rnl", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ttp", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("imploc", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ordlang", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("operation_system", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("version", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lastvisittime", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pvn", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("vnum", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pagedur", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("session", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("subpt", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("currentDate", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);

            HiveContext hiveContext = broadcastHC.value();
            DataFrame df = hiveContext.createDataFrame(rdd, structType);
            df.registerTempTable("tmp_ods_log_pageview");

            hiveContext.sql("set hive.exec.dynamic.partition=true");
            hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            hiveContext.sql("INSERT INTO sparkstreaming.tmp_ods_log_pageview PARTITION (dt) select * from tmp_ods_log_pageview");
        }
        return null;
    }
}
