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

public class InsertOdsLogProdExpo implements Function2<JavaRDD<Row>, Time, Void> {

    private Broadcast<HiveContext> broadcastHC;

    public InsertOdsLogProdExpo(Broadcast<HiveContext> broadcastHC) {
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
            structFields.add(DataTypes.createStructField("pid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pic", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("dispcid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pname", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ptype", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pos", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("price", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("page", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("imploc", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("plink", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("supplierid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sname", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("schtype", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ordlang", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("operation_system", DataTypes.StringType, true));
            //structFields.add(DataTypes.createStructField("schkw", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("psum", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("feedback", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("qs", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("slink", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("proctime", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("minorder", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ifspring", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("iffreeship", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ifstock", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("iffac", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ifcou", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ifdp", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("if48h", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ifgold", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ifban", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("online", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("msge", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("bpic", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("prozone", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pubcid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("srs", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("adgroup_id", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("campaign_id", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);

            HiveContext hiveContext = broadcastHC.value();
            DataFrame df = hiveContext.createDataFrame(rdd, structType);
            df.registerTempTable("tmp_ods_log_prod_expo");

            hiveContext.sql("set hive.exec.dynamic.partition=true");
            hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            hiveContext.sql("insert into sparkstreaming.tmp_ods_log_prod_expo partition(dt) select * from tmp_ods_log_prod_expo");
        }
        return null;
    }
}
