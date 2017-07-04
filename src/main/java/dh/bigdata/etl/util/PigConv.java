package dh.bigdata.etl.util;

import com.dhgate.event.DHEvent;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PigConv {

	public static String getSite(DHEvent event) throws Exception {
		String site = null;
		if (event.getTags().get("site") != null && !"".equals(event.getTags().get("site").toString())) {
			site = event.getTags().get("site").toString();
		} else {
			String siteTmp = getSitebyUrl(event.getU());
			if (siteTmp == null || "".equals(siteTmp)) {
				site = "www";
			} else {
				site = siteTmp;
			}
		}
		return site;
	}

	public static String getLang(DHEvent event) throws Exception {
		String lang = null;
		if (event.getTags().get("lang") != null && !"".equals(event.getTags().get("lang").toString())) {
			lang = event.getTags().get("lang").toString();
		} else {
			String langTmp = getLangbyUrl(event.getU());
			if (langTmp == null || "".equals(langTmp)) {
				lang = "en";
			} else {
				lang = langTmp;
			}
		}
		return lang;
	}

	public static String getSitebyUrl(String currentUrl) throws IOException {
		if (currentUrl == null) {
			return null;
		}
		String site = "";
		try {
			String u = currentUrl;
			if (u != null) {
				if (u.matches("http[s]?://factory.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://seller.dhgate.com.*") || u.matches("http[s]?://adcenter.dhgate.com.*")
						|| u.matches("http[s]?://policy.dhgate.com.*") || u.matches("http[s]?://elady.dhgate.com.*")
						|| u.matches("http[s]?://bbs.dhgate.com.*") || u.matches("http[s]?://open.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://supplier.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://m.dhgate.com.*")) {
					site = "wap";
				} else if (u.matches("http[s]?://m.ru.dhgate.com.*")) {
					site = "wap";
				} else if (u.matches("http[s]?://m.de.dhgate.com.*")) {
					site = "wap";
				} else if (u.matches("http[s]?://m.es.dhgate.com.*")) {
					site = "wap";
				} else if (u.matches("http[s]?://m.it.dhgate.com.*")) {
					site = "wap";
				} else if (u.matches("http[s]?://m.pt.dhgate.com.*")) {
					site = "wap";
				} else if (u.matches("http[s]?://m.fr.dhgate.com.*")) {
					site = "wap";
				} else if (u.matches("http[s]?://m.tr.dhgate.com.*")) {
					site = "wap";
				} else if (u.matches("http[s]?://ru.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://pt.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://es.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://fr.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://de.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://it.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://tr.dhgate.com.*")) {
					site = "www";
				} else if (u.matches("http[s]?://app.dhgate.com.*")) {
					site = "app";
				} else if (u.matches(".*dhport.com.*")) {
					site = "dhp";
				} else {
					site = "www";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return site;
	}

	public static String getLangbyUrl(String currentUrl) throws IOException {
		if (currentUrl == null) {
			return null;
		}
		String lan = "";
		try {
			String u = currentUrl;
			if (u != null) {
				if (u.matches("http[s]?://factory.dhgate.com.*")) {
					lan = "";
				} else if (u.matches("http[s]?://seller.dhgate.com.*") || u.matches("http[s]?://adcenter.dhgate.com.*")
						|| u.matches("http[s]?://policy.dhgate.com.*") || u.matches("http[s]?://elady.dhgate.com.*")
						|| u.matches("http[s]?://bbs.dhgate.com.*") || u.matches("http[s]?://open.dhgate.com.*")) {
					lan = "";
				} else if (u.matches("http[s]?://supplier.dhgate.com.*")) {
					lan = "";
				} else if (u.matches("http[s]?://m.dhgate.com.*")) {
					lan = "en";
				} else if (u.matches("http[s]?://m.ru.dhgate.com.*")) {
					lan = "ru";
				} else if (u.matches("http[s]?://m.de.dhgate.com.*")) {
					lan = "de";
				} else if (u.matches("http[s]?://m.es.dhgate.com.*")) {
					lan = "es";
				} else if (u.matches("http[s]?://m.it.dhgate.com.*")) {
					lan = "it";
				} else if (u.matches("http[s]?://m.pt.dhgate.com.*")) {
					lan = "pt";
				} else if (u.matches("http[s]?://m.fr.dhgate.com.*")) {
					lan = "fr";
				} else if (u.matches("http[s]?://m.tr.dhgate.com.*")) {
					lan = "tr";
				} else if (u.matches("http[s]?://ru.dhgate.com.*")) {
					lan = "ru";
				} else if (u.matches("http[s]?://pt.dhgate.com.*")) {
					lan = "pt";
				} else if (u.matches("http[s]?://es.dhgate.com.*")) {
					lan = "es";
				} else if (u.matches("http[s]?://fr.dhgate.com.*")) {
					lan = "fr";
				} else if (u.matches("http[s]?://de.dhgate.com.*")) {
					lan = "de";
				} else if (u.matches("http[s]?://it.dhgate.com.*")) {
					lan = "it";
				} else if (u.matches("http[s]?://tr.dhgate.com.*")) {
					lan = "tr";
				} else if (u.matches("http[s]?://app.dhgate.com.*")) {
					lan = "";
				} else {
					lan = "en";
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return lan;
	}

	public static String getUuid(String type, String url, String uuid) throws Exception{
		String Uuid = "";
		String type7c = type.replace("%7C", "|");
		String url7c = url.replace("%7C", "|");
		if(isValidString(type) && type7c.matches(".*\\|[\\d{6,12}].*")){
			Uuid = reg_extract(type7c, ".*\\|(\\d*).*");
			return Uuid;
		} else if(isValidString(type) && type7c.matches(".*\\|.*\\:.*\\:r\\d{6,12}.*")){
			Uuid = reg_extract(type7c, ".*\\|.*\\:.*\\:(r\\d{6,12}).*");
			return Uuid;
		} else if(isValidString(url) && url7c.matches(".*\\|.*\\:.*\\:r\\d{6,12}.*")){
			Uuid = reg_extract(url7c, ".*\\|.*\\:.*\\:(r\\d{6,12}).*");
			return Uuid;
		} else if(isValidString(uuid)){
			Uuid = uuid;
			return Uuid;
		} else {
			Uuid = "";
		}
		return Uuid;
	}

	public static String reg_extract (String src, String pat) throws Exception {
		Pattern pattern = Pattern.compile(pat);
		Matcher matcher = pattern.matcher(src);
		String result = "";
		if (matcher.matches()) {
			result = matcher.group(1);
		}
		return result;
	}

	public static boolean isValidString(String str) {
		if ((null != str) && (!"".equals(str))) {
			return true;
		} else {
			return false;
		}
	}

	public static void main(String[] args) throws Exception{
		String str = null;
		System.out.println(isValidString(str));
		System.out.println(getUuid("1;searl|592625484", "", "111"));
		System.out.println(getUuid("3|ff808081463705700146478613b15c07:108:r1413242196", "http://m.dhgate.com/mobileApiWeb/cart-Cart-addToCart.do", "111"));
		System.out.println(getUuid("1;searl%7C592625484", "", "111"));
		System.out.println(getUuid("3%7Cff808081463705700146478613b15c07:108:r1413242196", "http://m.dhgate.com/mobileApiWeb/cart-Cart-addToCart.do", "111"));
		System.out.println(getUuid("3%7Cff808081463705700146478613b15c07:108:r14132asdg196", "http://m.dhgate.com/mobileApiWeb/cart-Cart-addToCart.do", "111"));
		System.out.println(getUuid("3%7Cff808081463705700146478613b15c07:108:r14132asdg196", "http://m.dhgate.com/mobileApiWeb/cart-Cart-addToCart.do#3%7Cff808081463705700146478613b15c07:108:r1413242196", ""));
	}

}