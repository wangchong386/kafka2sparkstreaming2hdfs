package dh.bigdata.etl.sparkstreaming;

import com.dhgate.event.DHEvent;
import dh.bigdata.etl.util.PigConv;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogApp implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_app中的行
     * @param event
     * @return
     * @throws Exception
     */
    @Override
    public Row call(DHEvent event) throws Exception {
        return RowFactory.create(event.getName(),
                event.getVid(),
                event.getUsrid(),
                event.getVt(),
                event.getCou(),
                event.getUa(),
                event.getF(),
                event.getUlevel(),
                event.getAid(),
                (event.getTags().get("position") == null) ? "" : event.getTags().get("position").toString(),
                (event.getTags().get("loc") == null) ? "" : event.getTags().get("loc").toString(),
                (event.getTags().get("screen") == null) ? "" : event.getTags().get("screen").toString(),
                (event.getTags().get("clientid") == null) ? "" : event.getTags().get("clientid").toString(),
                (event.getTags().get("categoryid") == null) ? "" : event.getTags().get("categoryid").toString(),
                (event.getTags().get("itemcode") == null) ? "" : event.getTags().get("itemcode").toString(),
                (event.getTags().get("field13") == null) ? "" : event.getTags().get("field13").toString(),
                (event.getTags().get("field14") == null) ? "" : event.getTags().get("field14").toString(),
                (event.getTags().get("field15") == null) ? "" : event.getTags().get("field15").toString()
        );
    }
}
