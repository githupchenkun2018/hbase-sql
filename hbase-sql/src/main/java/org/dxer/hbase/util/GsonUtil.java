package org.dxer.hbase.util;

import com.google.gson.Gson;

/**
 * ClassName: GsonUtil <br/>
 * Function: <br/>
 * date: 2019/4/4 15:48 <br/>
 *
 * @author chenk
 * @since JDK 1.8
 */
public class GsonUtil {
    private static final Gson GSON = new Gson();

    public static String toStr(Object o) {
        if(null != o) {
            return GSON.toJson(o);
        }

        return "";
    }
}
