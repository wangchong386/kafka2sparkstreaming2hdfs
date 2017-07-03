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

public class InsertOdsLogRecs implements Function2<JavaRDD<Row>, Time, Void> {

    private Broadcast<HiveContext> broadcastHC;

    public InsertOdsLogRecs(Broadcast<HiveContext> broadcastHC) {
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
            structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("operation_system", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("browser_version", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("imploc", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("browser", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pic", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("algo", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("cytpe", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("currentDate", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);

            HiveContext hiveContext = broadcastHC.value();
            DataFrame df = hiveContext.createDataFrame(rdd, structType);
            df.registerTempTable("tmp_ods_log_rec_s");

            hiveContext.sql("set hive.exec.dynamic.partition=true");
            hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            hiveContext.sql("INSERT INTO sparkstreaming.tmp_ods_log_rec_s PARTITION (dt) select * from tmp_ods_log_rec_s");
        }
        return null;
    }
}
