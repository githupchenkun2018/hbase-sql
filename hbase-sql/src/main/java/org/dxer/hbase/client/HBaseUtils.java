package org.dxer.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.dxer.hbase.HBaseSqlContants;
import org.dxer.hbase.exceptions.NoRowKeyException;
import org.dxer.hbase.sql.util.ExpressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class HBaseUtils {
    private static Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);

    public static List<Result> getResults(Connection connection, String tableName, Scan scan, Long offset, Long rowCount, Map<String, List<String>> resultColumn) {
        ResultScanner rs = null;
        Table table = null;
        List<Result> results = null;
        try {
            FilterList filterList = (FilterList) scan.getFilter();
            // 不支持全表查询，必须要有条件
            if (filterList == null || filterList.getFilters().isEmpty()) {
                return results;
            }

            table = getHtable(connection, tableName);  // 从表池中取出HBASE表对象
            rs = table.getScanner(scan);

            // 遍历扫描器对象， 并将需要查询出来的数据row key取出
            if (rs != null) {
                results = new ArrayList<Result>();
                for (Result result : rs) {
                    results.add(result);
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            closeScanner(rs);
            closeTable(table);
            closeConnection(connection);
        }

        return results;
    }

    public static Table getHtable(Connection connection, String tableName) {
        if (connection == null) {
            return null;
        }

        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * HBase表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean tableExists(HConnection connection, String tableName) {
        boolean isexists = false;
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(HBaseConfig.getConfiguration());

            isexists = admin.tableExists(tableName);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            closeTable(admin);
        }
        return isexists;
    }

    public static byte[] getBytes(String str) {
        if (!Strings.isNullOrEmpty(str)) {
            return Bytes.toBytes(str);
        }
        return null;
    }

    public static String toStr(byte[] b) {
        if (b != null) {
            return new String(b);
        }
        return null;
    }

    public static List<Get> getListByRowKeyBytes(List<byte[]> rowKeyByteList, Map<String, List<String>> resultColumn) {
        List<Get> list = new LinkedList<Get>();
        if (rowKeyByteList == null || rowKeyByteList.size() == 0) {
            return list;
        }
        for (byte[] row : rowKeyByteList) {
            if (row == null) {
                continue;
            }
            Get get = new Get(row);

            if (resultColumn != null) {
                Set<String> cfSet = resultColumn.keySet();
                for (String cf : cfSet) {
                    List<String> cList = resultColumn.get(cf);
                    for (String c : cList) {
                        get.addColumn(getBytes(cf), getBytes(c));
                    }
                }
            }

            list.add(get);
        }
        return list;
    }

    public static List<Get> getListByRowKeys(List<String> rowKeyList, Map<String, List<String>> resultColumn) {
        if (rowKeyList != null && !rowKeyList.isEmpty()) {
            List<byte[]> rowKeyByteList = new ArrayList<byte[]>();
            for (String rowkey : rowKeyList) {
                rowKeyByteList.add(getBytes(rowkey));
            }

            return getListByRowKeyBytes(rowKeyByteList, resultColumn);
        }
        return null;
    }

    private static int getTotalPage(int pageSize, int totalCount) {
        int n = totalCount / pageSize;
        if (totalCount % pageSize == 0) {
            return n;
        } else {
            return ((int) n) + 1;
        }
    }

    /**
     * put result
     *
     * @param r
     */
    public static void printResult(Result r) {
        if (r == null || r.isEmpty()) {
            LOG.debug("result is null or empty!");
        } else {
            StringBuilder sb = new StringBuilder();
            for (Cell cell : r.rawCells()) {
                sb.append("Row=" + Bytes.toString(r.getRow()) + "\t\t");
                sb.append("column=" + Bytes.toString(CellUtil.cloneFamily(cell)) + "." +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + ", ");
                sb.append("timestamp=" + cell.getTimestamp() + ", ");
                sb.append("value=" + Bytes.toString(CellUtil.cloneValue(cell)) + "\n");
            }
            System.out.println(sb.toString());
            LOG.debug(sb.toString());
        }
    }

    /**
     * build put for hbase
     *
     * @param map
     * @return
     * @throws NoRowKeyException
     */
    public static Put build(Map<String, String> map) throws NoRowKeyException {
        Put put = null;
        if (map == null || map.isEmpty()) {
            return put;
        }

        String rowKey = map.get(HBaseSqlContants.ROW_KEY);

        if (Strings.isNullOrEmpty(rowKey)) {
            for (String key : map.keySet()) {
                if (HBaseSqlContants.ROW_KEY.equals(key.toUpperCase())) {
                    rowKey = map.get(key);
                    break;
                }
            }
        }

        if (Strings.isNullOrEmpty(rowKey)) {
            throw new NoRowKeyException("insert data but no rowkey");
        }

        put = new Put(Bytes.toBytes(rowKey));

        for (String key : map.keySet()) {
            if (HBaseSqlContants.ROW_KEY.equals(key.toUpperCase())) {
                continue;
            }

            String family = null;
            String column = null;

            String[] columnGroup = ExpressionUtil.getColumnGroup(key);
            if (columnGroup != null && columnGroup.length == 2) {
                family = columnGroup[0];
                column = columnGroup[1];
            }

            String value = map.get(key);

            if (!Strings.isNullOrEmpty(family) && !Strings.isNullOrEmpty(column) && !Strings.isNullOrEmpty(value)) {
                put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            }
        }

        return put;
    }

    /**
     * close scanner
     *
     * @param scanner
     */
    private static void closeScanner(ResultScanner scanner) {
        if (scanner != null)
            scanner.close();
    }

    /**
     * close
     *
     * @param o
     */
    private static void closeTable(Closeable o) {
        if (o != null) {
            try {
                o.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private static void closeConnection(Connection o) {
        if (o != null) {
            try {
                o.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
