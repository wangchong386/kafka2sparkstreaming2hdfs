package dh.bigdata.etl.sparkstreaming;

import com.dhgate.event.DHEvent;
import dh.bigdata.etl.util.PigConv;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Event2OdsLogSearchs implements Function<DHEvent, Row> {

    /**
     * 将DHEvent转换成tmp_ods_log_search_s中的行
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
                (event.getTags().get("schkw") == null) ? "" : event.getTags().get("schkw").toString(),
                (event.getTags().get("schtype") == null) ? "" : event.getTags().get("schtype").toString(),
                (event.getTags().get("dcid") == null) ? "" : event.getTags().get("dcid").toString(),
                (event.getTags().get("cid") == null) ? "" : event.getTags().get("cid").toString(),
                (event.getTags().get("recomdcids") == null) ? "" : event.getTags().get("recomdcids").toString(),
                (event.getTags().get("cids") == null) ? "" : event.getTags().get("cids").toString(),
                (event.getTags().get("ass") == null) ? "" : event.getTags().get("ass").toString(),
                (event.getTags().get("sorttype") == null) ? "" : event.getTags().get("sorttype").toString(),
                (event.getTags().get("rsnum") == null) ? "" : event.getTags().get("rsnum").toString(),
                (event.getTags().get("pagenum") == null) ? "" : event.getTags().get("pagenum").toString(),
                (event.getTags().get("page") == null) ? "" : event.getTags().get("page").toString(),
                (event.getTags().get("itemnum") == null) ? "" : event.getTags().get("itemnum").toString(),
                (event.getTags().get("relsch") == null) ? "" : event.getTags().get("relsch").toString(),
                (event.getTags().get("ft_vipdiscount") == null) ? "" : event.getTags().get("ft_vipdiscount").toString(),
                (event.getTags().get("ft_customerreview") == null) ? "" : event.getTags().get("ft_customerreview").toString(),
                (event.getTags().get("ft_minprice") == null) ? "" : event.getTags().get("ft_minprice").toString(),
                (event.getTags().get("ft_maxprice") == null) ? "" : event.getTags().get("ft_maxprice").toString(),
                PigConv.getSite(event),
                PigConv.getLang(event),
                (event.getTags().get("ordlang") == null) ? "" : event.getTags().get("ordlang").toString(),
                (event.getTags().get("operation_system") == null) ? "" : event.getTags().get("operation_system").toString(),
                search_keywords_md5,
                (event.getTags().get("abtest") == null) ? "" : event.getTags().get("abtest").toString(),
                (event.getTags().get("ft_dispro") == null) ? "" : event.getTags().get("ft_dispro").toString(),
                (event.getTags().get("ft_ispromotion") == null) ? "" : event.getTags().get("ft_ispromotion").toString(),
                (event.getTags().get("ft_oneday") == null) ? "" : event.getTags().get("ft_oneday").toString(),
                (event.getTags().get("ft_sevenreturn") == null) ? "" : event.getTags().get("ft_sevenreturn").toString(),
                (event.getTags().get("ft_wholesaleonly") == null) ? "" : event.getTags().get("ft_wholesaleonly").toString(),
                (event.getTags().get("ft_minorder") == null) ? "" : event.getTags().get("ft_minorder").toString(),
                (event.getTags().get("ft_freeshipping") == null) ? "" : event.getTags().get("ft_freeshipping").toString(),
                (event.getTags().get("ft_selleronline") == null) ? "" : event.getTags().get("ft_selleronline").toString(),
                (event.getTags().get("ft_descpledge") == null) ? "" : event.getTags().get("ft_descpledge").toString(),
                (event.getTags().get("gallery") == null) ? "" : event.getTags().get("gallery").toString(),
                (event.getTags().get("ft_shipall") == null) ? "" : event.getTags().get("ft_shipall").toString(),
                (event.getTags().get("ft_shipmethodnote") == null) ? "" : event.getTags().get("ft_shipmethodnote").toString(),
                (event.getTags().get("ft_shipcountry") == null) ? "" : event.getTags().get("ft_shipcountry").toString(),
                (event.getTags().get("ft_shipCountryName") == null) ? "" : event.getTags().get("ft_shipCountryName").toString(),
                (event.getTags().get("totalPro") == null) ? "" : event.getTags().get("totalPro").toString(),
                (event.getTags().get("crumbs") == null) ? "" : event.getTags().get("crumbs").toString(),
                (event.getTags().get("ft_singleonly") == null) ? "" : event.getTags().get("ft_singleonly").toString(),
                (event.getTags().get("algo") == null) ? "" : event.getTags().get("algo").toString(),
                (event.getTags().get("ft_sponsor") == null) ? "" : event.getTags().get("ft_sponsor").toString(),
                event.getTags().get("currentDate").toString());
    }
}
