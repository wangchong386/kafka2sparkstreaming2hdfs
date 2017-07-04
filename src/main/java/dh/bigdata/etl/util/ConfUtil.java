package dh.bigdata.etl.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class ConfUtil {

    private Properties prop = new Properties();
    static private ConfUtil confUtil = null;

    private ConfUtil() {
        load();
    }

    public static ConfUtil getInstance() {
        if (confUtil == null) {
            new ConfUtilHolder();
        }
        return confUtil;
    }

    private void load() {
        try {
            InputStream in = new BufferedInputStream(new FileInputStream("etl_dh_data.properties"));
            prop.load(in);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getProperty(String var) {
        return prop.getProperty(var);
    }

    private static class ConfUtilHolder {

        static {
            confUtil = new ConfUtil();
        }
    }
}
