package org.dxer.hbase.sql.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.dxer.hbase.HBaseSqlContants;

import com.google.common.base.Strings;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;

public class ExpressionUtil {

    /**
     * 解决jsqlParser解析的bug
     * @param str
     * @return
     */
    public static String convert(String str) {
        if(Strings.isNullOrEmpty(str)) {
            return str;
        }

        return replaceSymbol(str);
    }

    public static String convertColumnQualifier(String str) {
//        if(Strings.isNullOrEmpty(str)) {
//            return str;
//        }
//
//        if(!(str.startsWith(HBaseSqlContants.QUOTE) && str.endsWith(HBaseSqlContants.QUOTE))) {
//          str = HBaseSqlContants.QUOTE + str + HBaseSqlContants.QUOTE;
//        }

//        return str.replace(".", ":");
        return str;
    }

    private static String replaceSymbol(String str) {
        str = str.replace(HBaseSqlContants.BUG_1, HBaseSqlContants.BUG_1_FIX);
        return str;
    }

    public static List<String> getStringList(ItemsList itemsList) {
        List<String> list = null;
        if (itemsList != null) {
            List items = ((ExpressionList) itemsList).getExpressions();
            if (items != null && items.size() > 0) {
                list = new ArrayList<String>();
                for (Object o : items) {
                    String value = getString((Expression) o);

                    if (!Strings.isNullOrEmpty(value)) {
                        list.add(value);
                    }
                }
            }
        }
        return list;
    }

    public static String[] getColumnGroup(String columnGroupStr) {
        if (!Strings.isNullOrEmpty(columnGroupStr)) {
            String[] strs = columnGroupStr.split("\\.");
            if (strs != null && strs.length == 2) {
                return strs;
            }
        }
        return null;
    }

    public static void setColumnMap(String columnGroup, Map<String, List<String>> columnMap) {
        if (Strings.isNullOrEmpty(columnGroup) || columnMap == null) {
            return;
        }

        String[] strs = getColumnGroup(columnGroup);
        if (strs != null && strs.length == 2) {
            String family = strs[0];
            String column = strs[1];

            if (!Strings.isNullOrEmpty(family) && !Strings.isNullOrEmpty(column)) {
                List<String> columns = columnMap.get(family);
                if (columns != null) {
                    columns.add(column);
                } else {
                    columns = new ArrayList<String>();
                    columns.add(column);
                }
                columnMap.put(family, columns);
            }
        }
    }


    public static String getString(Object o) {
        String value = null;
        if (o != null) {
            if (o instanceof LongValue) {
                LongValue longValue = (LongValue) o;
                value = ((LongValue) longValue).getStringValue();
            } else if (o instanceof StringValue) {
                StringValue stringValue = (StringValue) o;
                value = stringValue.getValue();
            } else if (o instanceof DoubleValue) {
                DoubleValue doubleValue = (DoubleValue) o;
                value = ((DoubleValue) doubleValue).getValue() + "";
            }
        }
        return value;
    }

    public static String getString(Expression expression) {
        String value = null;
        if (expression != null) {
            value = expression.toString();
        }

        return value;
    }

}
