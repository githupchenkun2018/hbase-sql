package org.dxer.hbase.util;

import org.apache.hadoop.hbase.util.Strings;

public class RowKeyUtil {

    public String reverse(String s) {
        String ret = null;
        if (!Strings.isEmpty(s)) {
            StringBuilder sb = new StringBuilder(s);
            ret = sb.reverse().toString();
        }
        return ret;
    }


}
