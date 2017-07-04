package dh.bigdata.etl.sparkstreaming;

import com.dhgate.event.DHEvent;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogAppEvents implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_app_events中的行
     * @param event
     * @return
     * @throws Exception
     */
    @Override
    public Row call(DHEvent event) throws Exception {
        return RowFactory.create(event.getName(),
                event.getVt(),
                (event.getTags().get("imei") == null) ? "" : event.getTags().get("imei").toString(),
                (event.getTags().get("guid") == null) ? "" : event.getTags().get("guid").toString(),
                (event.getTags().get("site") == null) ? "" : event.getTags().get("site").toString(),
                (event.getTags().get("currevent") == null) ? "" : event.getTags().get("currevent").toString(),
                (event.getTags().get("isfirst") == null) ? "" : event.getTags().get("isfirst").toString(),
                (event.getTags().get("eventtime") == null) ? "" : event.getTags().get("eventtime").toString(),
                (event.getTags().get("duration") == null) ? "" : event.getTags().get("duration").toString(),
                (event.getTags().get("defineid") == null) ? "" : event.getTags().get("defineid").toString(),
                (event.getTags().get("pageid") == null) ? "" : event.getTags().get("pageid").toString(),
                (event.getTags().get("prepageid") == null) ? "" : event.getTags().get("prepageid").toString(),
                (event.getTags().get("sessionid") == null) ? "" : event.getTags().get("sessionid").toString(),
                (event.getTags().get("userid") == null) ? "" : event.getTags().get("userid").toString(),
                (event.getTags().get("eventid") == null) ? "" : event.getTags().get("eventid").toString(),
                (event.getTags().get("pagedur") == null) ? "" : event.getTags().get("pagedur").toString(),
                (event.getTags().get("moduleid") == null) ? "" : event.getTags().get("moduleid").toString(),
                (event.getTags().get("modulecnt") == null) ? "" : event.getTags().get("modulecnt").toString(),
                (event.getTags().get("firstvt") == null) ? "" : event.getTags().get("firstvt").toString(),
                (event.getTags().get("lastvt") == null) ? "" : event.getTags().get("lastvt").toString(),
                (event.getTags().get("tpvn") == null) ? "" : event.getTags().get("tpvn").toString(),
                (event.getTags().get("svum") == null) ? "" : event.getTags().get("svum").toString(),
                (event.getTags().get("tclk") == null) ? "" : event.getTags().get("tclk").toString(),
                (event.getTags().get("sclk") == null) ? "" : event.getTags().get("sclk").toString(),
                (event.getTags().get("activityid") == null) ? "" : event.getTags().get("activityid").toString(),
                (event.getTags().get("pushid") == null) ? "" : event.getTags().get("pushid").toString(),
                (event.getTags().get("d1coce") == null) ? "" : event.getTags().get("d1coce").toString(),
                (event.getTags().get("itemcode") == null) ? "" : event.getTags().get("itemcode").toString(),
                (event.getTags().get("ptype") == null) ? "" : event.getTags().get("ptype").toString(),
                (event.getTags().get("coupon") == null) ? "" : event.getTags().get("coupon").toString(),
                (event.getTags().get("rfxno") == null) ? "" : event.getTags().get("rfxno").toString(),
                (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                (event.getTags().get("schkw") == null) ? "" : event.getTags().get("schkw").toString(),
                (event.getTags().get("f") == null) ? "" : event.getTags().get("f").toString(),
                (event.getTags().get("lang") == null) ? "" : event.getTags().get("lang").toString()
        );
    }
}
