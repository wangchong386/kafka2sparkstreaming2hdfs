package dh.bigdata.etl.sparkstreaming;

import com.dhgate.event.DHEvent;
import dh.bigdata.etl.util.PigConv;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogSubjectExpo implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_subject_expo中的行
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
                event.getRefpvid(),
                event.getPvid(),
                event.getUlevel(),
                event.getAid(),
                event.getU(),
                event.getCou(),
                PigConv.getSite(event),
                PigConv.getLang(event),
                (event.getTags().get("uuid") == null) ? "" : event.getTags().get("uuid").toString(),
                (event.getTags().get("pos") == null) ? "" : event.getTags().get("pos").toString(),
                (event.getTags().get("subjectid") == null) ? "" : event.getTags().get("subjectid").toString(),
                (event.getTags().get("subjecttype") == null) ? "" : event.getTags().get("subjecttype").toString(),
                (event.getTags().get("subjectextend") == null) ? "" : event.getTags().get("subjectextend").toString(),
                (event.getTags().get("subjecturl") == null) ? "" :event.getTags().get("subjecturl").toString(),
                (event.getTags().get("subjectname") == null) ? "" : event.getTags().get("subjectname").toString(),
                (event.getTags().get("subjecttitle") == null) ? "" : event.getTags().get("subjecttitle").toString(),
                (event.getTags().get("subjectsubtitle") == null) ? "" : event.getTags().get("subjectsubtitle").toString(),
                event.getTags().get("currentDate").toString());
    }
}
