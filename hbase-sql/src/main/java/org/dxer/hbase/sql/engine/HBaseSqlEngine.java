package org.dxer.hbase.sql.engine;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.dxer.hbase.entity.HResult;

public interface HBaseSqlEngine {
    List<Result> select(String sql) throws Exception;
    List<HResult> selectHResult(String sql) throws Exception;
}
