package org.apache.hbase.client;


import org.dxer.hbase.sql.engine.HBaseSqlEngine;
import org.dxer.hbase.sql.engine.impl.HBaseSqlEngineImpl;

public class Test {
    public static void main(String[] args) throws Exception {
        testWhere();
    }

    private static void testWhere() throws Exception {
//        String sql = "select * from rms_uat01_ns.risk_param_special_aggr where _rowkey_ in (F_11_-9999,F_212_0202020012)"
//                + " and _column_ >= 'statistics:chanCodeCashAppl_20190104' " +
//                "and  " ;
        StringBuilder sb = new StringBuilder();
        sb.append("select * from rms_uat01_ns.risk_param_phone where ");
        sb.append("_rowkey_ = E_00_15111111112");
        sb.append(" and _column_ like pwdRecTrans");
//        sb.append(" and _column_ < chanCodeCashAppl_20190116");

        HBaseSqlEngine engine = new HBaseSqlEngineImpl();
        engine.select(sb.toString());

        System.out.println(sb.toString());

//        CCJSqlParserManager parserManager = new CCJSqlParserManager();
//        Select select = (Select) parserManager.parse(new StringReader(sql));
//        SelectBody selectBody = (PlainSelect)select.getSelectBody();
//        SelectDeParser selectDeParser = new SelectDeParser();
//        selectBody.accept(selectDeParser);
//        selectBody.toString();
//        selectDeParser.toString();
    }

//    HBaseSqlEngine sqlEngine = new HBaseSqlEngineImpl();
//    String sql = "select * from  user where _rowkey_=111";
//    //   String sql = "select * from user where _rowkey_ in (111, 222)";
////        String sql = "select info.age from user where _pre_rowkey_  = 11 ";
//
//    List<HResult> results = sqlEngine.select(sql);
//
//        for (HResult r : results) {
//        System.out.println(r.toString());
//    }
//
////        String s = "insert into user (_rowkey_, info.name, info.age) values ('sdfdsfsd', 'fdsfsd', 12)";
////        query.insert(s);
////        System.out.println("11111");
//
////        String s = "delete from user where _rowkey_ in ('111','22232','3333') and _column_ in ('info.name', 'info.age')";
////        query.del(s);
}
