package dh.bigdata.etl.sparkstreaming;

import com.dhgate.event.DHEvent;
import dh.bigdata.etl.util.PigConv;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogRecs implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_rec_s中的行
     * @param event
     * @return
     * @throws Exception
     */
    @Override
    public Row call(DHEvent event) throws Exception {

        return RowFactory.create(event.getName(),
                event.getId(),
                event.getVid(),
                event.getUsrid(),
                event.getSid(),
                event.getVt(),
                event.getIp(),
                event.getUa(),
                event.getF(),
                event.getD(),
                event.getRefurl(),
                event.getUlevel(),
                event.getAid(),
                event.getU(),
                event.getCou(),
                (event.getTags().get("uuid") == null) ? "" : event.getTags().get("uuid").toString(),
                (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                PigConv.getSite(event),
                PigConv.getLang(event),
                (event.getTags().get("operation_system") == null) ? "" : event.getTags().get("operation_system").toString(),
                (event.getTags().get("browser_version") == null) ? "" : event.getTags().get("browser_version").toString(),
                (event.getTags().get("imploc") == null) ? "" : event.getTags().get("imploc").toString(),
                (event.getTags().get("browser") == null) ? "" : event.getTags().get("browser").toString(),
                (event.getTags().get("pic") == null) ? "" : event.getTags().get("pic").toString(),
                (event.getTags().get("algo") == null) ? "" : event.getTags().get("algo").toString(),
                (event.getTags().get("cytpe") == null) ? "" : event.getTags().get("cytpe").toString(),
                event.getTags().get("currentDate").toString());
    }

}
