package dh.bigdata.etl.sparkstreaming;

import com.dhgate.event.DHEvent;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogAppDevice implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_app_device中的行
     * @param event
     * @return
     * @throws Exception
     */
    @Override
    public Row call(DHEvent event) throws Exception {
        return RowFactory.create(event.getName(),
                (event.getTags().get("sessionid") == null) ? "" : event.getTags().get("sessionid").toString(),
                event.getVt(),
                (event.getTags().get("site") == null) ? "" : event.getTags().get("site").toString(),
                (event.getTags().get("appid") == null) ? "" : event.getTags().get("appid").toString(),
                (event.getTags().get("appver") == null) ? "" : event.getTags().get("appver").toString(),
                (event.getTags().get("channel") == null) ? "" : event.getTags().get("channel").toString(),
                (event.getTags().get("sdkver") == null) ? "" : event.getTags().get("sdkver").toString(),
                (event.getTags().get("userid") == null) ? "" : event.getTags().get("userid").toString(),
                (event.getTags().get("guid") == null) ? "" : event.getTags().get("guid").toString(),
                (event.getTags().get("imei") == null) ? "" : event.getTags().get("imei").toString(),
                (event.getTags().get("module") == null) ? "" : event.getTags().get("module").toString(),
                (event.getTags().get("screen") == null) ? "" : event.getTags().get("screen").toString(),
                (event.getTags().get("density") == null) ? "" : event.getTags().get("density").toString(),
                (event.getTags().get("mac") == null) ? "" : event.getTags().get("mac").toString(),
                (event.getTags().get("udid") == null) ? "" : event.getTags().get("udid").toString(),
                (event.getTags().get("advertid") == null) ? "" : event.getTags().get("advertid").toString(),
                (event.getTags().get("vendorid") == null) ? "" : event.getTags().get("vendorid").toString(),
                (event.getTags().get("ismobile") == null) ? "" : event.getTags().get("ismobile").toString(),
                (event.getTags().get("isp") == null) ? "" : event.getTags().get("isp").toString(),
                (event.getTags().get("ip") == null) ? "" : event.getTags().get("ip").toString(),
                (event.getTags().get("nettype") == null) ? "" : event.getTags().get("nettype").toString(),
                (event.getTags().get("netstatus") == null) ? "" : event.getTags().get("netstatus").toString(),
                (event.getTags().get("sim") == null) ? "" : event.getTags().get("sim").toString(),
                (event.getTags().get("tel") == null) ? "" : event.getTags().get("tel").toString(),
                (event.getTags().get("longitude") == null) ? "" : event.getTags().get("longitude").toString(),
                (event.getTags().get("latitude") == null) ? "" : event.getTags().get("latitude").toString(),
                (event.getTags().get("locprov") == null) ? "" : event.getTags().get("locprov").toString(),
                (event.getTags().get("platform") == null) ? "" : event.getTags().get("platform").toString(),
                (event.getTags().get("os") == null) ? "" : event.getTags().get("os").toString(),
                (event.getTags().get("iscrack") == null) ? "" : event.getTags().get("iscrack").toString(),
                (event.getTags().get("ln") == null) ? "" : event.getTags().get("ln").toString(),
                (event.getTags().get("timezone") == null) ? "" : event.getTags().get("timezone").toString(),
                (event.getTags().get("requesttime") == null) ? "" : event.getTags().get("requesttime").toString()
        );
    }
}
