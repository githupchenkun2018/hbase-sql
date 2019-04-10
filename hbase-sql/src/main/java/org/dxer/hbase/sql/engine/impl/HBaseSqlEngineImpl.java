package org.dxer.hbase.sql.engine.impl;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLSyntaxErrorException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.dxer.hbase.HBaseSqlContants;
import org.dxer.hbase.client.HBaseUtils;
import org.dxer.hbase.entity.HResult;
import org.dxer.hbase.sql.engine.HBaseSqlEngine;
import org.dxer.hbase.sql.visitor.SelectSqlVisitor;
import org.dxer.hbase.util.GsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.Select;

public class HBaseSqlEngineImpl implements HBaseSqlEngine {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSqlEngineImpl.class);

    /**
     * 获取HConnection
     *
     * @return
     */
    public Connection getHConnection() {
        Connection connection = null;
        try {
            Configuration config = new Configuration();
            config.set("hbase.zookeeper.quorum", "10.10.8.101,10.10.8.102,10.10.8.103");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("zookeeper.znode.parent","/hbase-unsecure");
            Configuration configuration = HBaseConfiguration.create(config);
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }

        return connection;
    }

    /**
     * select
     *
     * @param sql
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public List<Result> select(String sql) throws Exception {
        Connection connection = getHConnection();
        if (connection == null) {
            LOG.error("hbase connection is null");
            throw new RuntimeException("connection is null");
        }

        SelectSqlVisitor sqlVisitor = parseSql(sql);
        String tableName = sqlVisitor.getTableName();
        Map<String, List<String>> queryColumnMap = sqlVisitor.getQueryColumnMap();
        Set<String> queryColumns = sqlVisitor.getQueryColumns();
        Scan scan = sqlVisitor.getScanner();

        //invalid
        Long offset = sqlVisitor.getOffset();
        //invalid
        Long rowCount = sqlVisitor.getRowCount();

        LOG.debug(GsonUtil.toStr(scan));
        List<Result> results = HBaseUtils.getResults(connection, tableName, scan, offset, rowCount, queryColumnMap);
        LOG.debug(GsonUtil.toStr(results));
        return results;
    }

    /**
     * select
     *
     * @param sql
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public List<HResult> selectHResult(String sql) throws Exception {
        Connection connection = getHConnection();
        if (connection == null) {
            LOG.error("hbase connection is null");
            throw new RuntimeException("HConnection is null");
        }

        SelectSqlVisitor sqlVisitor = parseSql(sql);
        String tableName = sqlVisitor.getTableName();
        Map<String, List<String>> queryColumnMap = sqlVisitor.getQueryColumnMap();
        Set<String> queryColumns = sqlVisitor.getQueryColumns();
        Scan scan = sqlVisitor.getScanner();

        //invalid
        Long offset = sqlVisitor.getOffset();
        //invalid
        Long rowCount = sqlVisitor.getRowCount();

        LOG.debug(GsonUtil.toStr(scan));
        List<Result> results = HBaseUtils.getResults(connection, tableName, scan, offset, rowCount, queryColumnMap);
        LOG.debug(GsonUtil.toStr(results));

        List<HResult> hResultList = null;
        // TODO 待完善
        if (results != null) {
            hResultList = new ArrayList<HResult>();
            for (Result result : results) {
                HResult hResult = getHResult(result, queryColumns);
                if (hResult != null) {
                    hResultList.add(hResult);
                }
            }
        }
        return hResultList;
    }

    private byte[] getBytes(String s) {
        if (!Strings.isNullOrEmpty(s)) {
            return Bytes.toBytes(s);
        }
        return null;
    }

    private HResult getHResult(Result result) {
        return getHResult(result, null);
    }

    private HResult getHResult(Result result, Set<String> queryColumns) {
        HResult hResult = null;
        if (result != null && !result.isEmpty()) {
            hResult = new HResult();
            Map<String, Object> resultMap = null;
            List<Cell> cells = result.listCells();
            if (cells != null) {
                resultMap = new HashMap<String, Object>();

                if (queryColumns != null &&
                        (queryColumns.contains(HBaseSqlContants.ROW_KEY) || queryColumns
                                .contains(HBaseSqlContants.ASTERISK))) { // 设置rowkey
                    String rowkey = Bytes.toString(result.getRow());
                    resultMap.put(HBaseSqlContants.ROW_KEY, rowkey);
                }

                for (Cell cell : cells) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell)); // family
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)); // qualifier

                    String column = family + "." + qualifier;
                    if (queryColumns == null ||
                            (!queryColumns.contains(column) && !queryColumns.contains(HBaseSqlContants.ASTERISK))) {
                        continue;
                    }

                    String value = Bytes.toString(CellUtil.cloneValue(cell)); // value

                    resultMap.put(family + "." + qualifier, value);

                    String columnWithTS = family + "." + qualifier + HBaseSqlContants.TS_SUFFIX;

                    if (queryColumns != null && queryColumns.contains(columnWithTS)) {
                        long ts = cell.getTimestamp(); // 时间戳
                        resultMap.put(columnWithTS, ts);
                    }
                }
                hResult.setResultMap(resultMap);
            }
        }
        return hResult;
    }

    private SelectSqlVisitor parseSql(String sql) throws SQLSyntaxErrorException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        SelectSqlVisitor sqlVisitor = null;
        try {
            Select select = (Select) parserManager.parse(new StringReader(sql));
            sqlVisitor = new SelectSqlVisitor(select);
        } catch (Exception e) {
            LOG.error("parseSql failed; cause: " + e.getMessage(), e);
            throw new SQLSyntaxErrorException(sql, e);
        }
        return sqlVisitor;
    }
}
