package dh.bigdata.etl.sparkstreaming;

import com.dhgate.event.DHEvent;
import dh.bigdata.etl.util.PigConv;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogProdExpo implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_prod_expo中的行
     * @param event
     * @return
     * @throws Exception
     */
    @Override
    public Row call(DHEvent event) throws Exception {
        String Schkw = null;
        Schkw = (event.getTags().get("schkw") == null) ? "" : event.getTags().get("schkw").toString();
        String search_keywords_md5 = ""; // PigConv.SchkwMD5(event.getSchkw());
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
                (event.getTags().get("pid") == null) ? "" : event.getTags().get("pid").toString(),
                (event.getTags().get("pic") == null) ? "" : event.getTags().get("pic").toString(),
                (event.getTags().get("dispcid") == null) ? "" : event.getTags().get("dispcid").toString(),
                (event.getTags().get("pname") == null) ? "" : event.getTags().get("pname").toString(),
                (event.getTags().get("ptype") == null) ? "" : event.getTags().get("ptype").toString(),
                (event.getTags().get("pos") == null) ? "" : event.getTags().get("pos").toString(),
                (event.getTags().get("price") == null) ? "" : event.getTags().get("price").toString(),
                (event.getTags().get("page") == null) ? "" : event.getTags().get("page").toString(),
                (event.getTags().get("imploc") == null) ? "" : event.getTags().get("imploc").toString(),
                (event.getTags().get("plink") == null) ? "" : event.getTags().get("plink").toString(),
                (event.getTags().get("supplierid") == null) ? "" : event.getTags().get("supplierid").toString(),
                (event.getTags().get("sname") == null) ? "" : event.getTags().get("sname").toString(),
                (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                (event.getTags().get("schtype") == null) ? "" : event.getTags().get("schtype").toString(),
                (event.getTags().get("schkw") == null) ? "" : event.getTags().get("schkw").toString(),
                PigConv.getSite(event),
                PigConv.getLang(event),
                (event.getTags().get("ordlang") == null) ? "" : event.getTags().get("ordlang").toString(),
                (event.getTags().get("operation_system") == null) ? "" : event.getTags().get("operation_system").toString(),
                (event.getTags().get("schkw") == null) ? "" : event.getTags().get("schkw").toString(),
                (event.getTags().get("psum") == null) ? "" : event.getTags().get("psum").toString(),
                (event.getTags().get("feedback") == null) ? "" : event.getTags().get("feedback").toString(),
                (event.getTags().get("qs") == null) ? "" : event.getTags().get("qs").toString(),
                (event.getTags().get("slink") == null) ? "" : event.getTags().get("slink").toString(),
                (event.getTags().get("proctime") == null) ? "" : event.getTags().get("proctime").toString(),
                (event.getTags().get("minorder") == null) ? "" : event.getTags().get("minorder").toString(),
                (event.getTags().get("ifspring") == null) ? "" : event.getTags().get("ifspring").toString(),
                (event.getTags().get("iffreeship") == null) ? "" : event.getTags().get("iffreeship").toString(),
                (event.getTags().get("ifstock") == null) ? "" : event.getTags().get("ifstock").toString(),
                (event.getTags().get("ifcou") == null) ? "" : event.getTags().get("ifcou").toString(),
                (event.getTags().get("ifdp") == null) ? "" : event.getTags().get("ifdp").toString(),
                (event.getTags().get("if48h") == null) ? "" : event.getTags().get("if48h").toString(),
                (event.getTags().get("ifgold") == null) ? "" : event.getTags().get("ifgold").toString(),
                (event.getTags().get("ifban") == null) ? "" : event.getTags().get("ifban").toString(),
                (event.getTags().get("online") == null) ? "" : event.getTags().get("online").toString(),
                (event.getTags().get("msge") == null) ? "" : event.getTags().get("msge").toString(),
                (event.getTags().get("bpic") == null) ? "" : event.getTags().get("bpic").toString(),
                (event.getTags().get("prozone") == null) ? "" : event.getTags().get("prozone").toString(),
                (event.getTags().get("pubcid") == null) ? "" : event.getTags().get("pubcid").toString(),
                (event.getTags().get("srs") == null) ? "" : event.getTags().get("srs").toString(),
                (event.getTags().get("adgroup_id") == null) ? "" : event.getTags().get("adgroup_id").toString(),
                (event.getTags().get("campaign_id") == null) ? "" : event.getTags().get("campaign_id").toString()
        );
    }
}
