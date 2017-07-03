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

public class InsertOdsLogSearchs implements Function2<JavaRDD<Row>, Time, Void> {

    private Broadcast<HiveContext> broadcastHC;

    public InsertOdsLogSearchs(Broadcast<HiveContext> broadcastHC) {
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
            structFields.add(DataTypes.createStructField("schkw", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("schtype", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("dcid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("cid", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("recomdcids", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("cids", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ass", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("sorttype", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("rsnum", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("pagenum", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("page", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("itemnum", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("relsch", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_vipdiscount", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_customerreview", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_minprice", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_maxprice", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("site", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("lang", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ordlang", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("operation_system", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("search_keywords_md5", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("abtest", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_dispro", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_ispromotion", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_oneday", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_sevenreturn", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_wholesaleonly", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_minorder", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_freeshipping", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_selleronline", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_descpledge", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("gallery", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_shipall", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_shipmethodnote", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_shipcountry", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_shipCountryName", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("totalPro", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("crumbs", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_singleonly", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("algo", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("ft_sponsor", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("currentDate", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);

            HiveContext hiveContext = broadcastHC.value();
            DataFrame df = hiveContext.createDataFrame(rdd, structType);
            df.registerTempTable("tmp_ods_log_search_s");

            hiveContext.sql("set hive.exec.dynamic.partition=true");
            hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            hiveContext.sql("insert into sparkstreaming.tmp_ods_log_search_s partition(dt) select * from tmp_ods_log_search_s");
        }
        return null;
    }
}
