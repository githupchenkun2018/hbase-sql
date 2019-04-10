package org.dxer.hbase.sql.visitor;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.dxer.hbase.HBaseSqlContants;
import org.dxer.hbase.sql.util.ExpressionUtil;

import com.google.common.base.Strings;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

public class SelectSqlVisitor implements SelectVisitor, FromItemVisitor, ExpressionVisitor {

    private List<String> tableNames = new ArrayList<String>();

    private Set<String> queryColumns = new HashSet<String>();

    private Map<String, List<String>> queryColumnMap = new HashMap<String, List<String>>();

    private FilterList filterList = new FilterList();

    private String startRow;

    private String stopRow;

    private Scan scanner = new Scan();

    private Long rowCount;

    private Long offset;

    public SelectSqlVisitor(Select select) {
        SelectBody selectBody = select.getSelectBody();
        selectBody.accept(this);
    }

    public String getTableName() {
        String sqlTable = tableNames.get(0);
        if(!Strings.isNullOrEmpty(sqlTable)) {
            sqlTable = sqlTable.replaceFirst("\\.", ":");
        }
        return sqlTable;
    }

    public Set<String> getQueryColumns() {
        return queryColumns;
    }

    public Map<String, List<String>> getQueryColumnMap() {
        return queryColumnMap;
    }

    public Long getOffset() {
        return offset;
    }

    public Long getRowCount() {
        return rowCount;
    }

    public Scan getScanner() {
        if (filterList != null && filterList.getFilters() != null && filterList.getFilters().size() > 0) {
            scanner.setFilter(filterList);
        }
        if (!Strings.isNullOrEmpty(startRow)) {
            scanner.setStartRow(Bytes.toBytes(startRow));
        }
        if (!Strings.isNullOrEmpty(stopRow)) {
            scanner.setStopRow(Bytes.toBytes(stopRow));
        }
        return scanner;
    }

    @Override
    public void visit(NullValue nullValue) {

    }

    @Override
    public void visit(Function function) {

    }

    @Override
    public void visit(InverseExpression inverseExpression) {

    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {

    }

    @Override
    public void visit(DoubleValue doubleValue) {

    }

    @Override
    public void visit(LongValue longValue) {

    }

    @Override
    public void visit(DateValue dateValue) {

    }

    @Override
    public void visit(TimeValue timeValue) {

    }

    @Override
    public void visit(TimestampValue timestampValue) {

    }

    @Override
    public void visit(Parenthesis parenthesis) {

    }

    @Override
    public void visit(StringValue stringValue) {

    }

    @Override
    public void visit(Addition addition) {

    }

    @Override
    public void visit(Division division) {

    }

    @Override
    public void visit(Multiplication multiplication) {

    }

    @Override
    public void visit(Subtraction subtraction) {

    }

    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        orExpression.getRightExpression().accept(this);
    }

    public void visit(Between between) {

    }

    public void visit(EqualsTo equalsTo) {
        String key = equalsTo.getLeftExpression().toString();
        String value = equalsTo.getRightExpression().toString();

        if(Strings.isNullOrEmpty(key) || Strings.isNullOrEmpty(value)) {
            return;
        }

        // fix bug 4 jsqlpraser
        value = ExpressionUtil.convert(value);

        CompareFilter.CompareOp op = CompareFilter.CompareOp.EQUAL;
        Filter filter = null;
        if (HBaseSqlContants.HBASE_COLUMN.equalsIgnoreCase(key)) {
            value = ExpressionUtil.convertColumnQualifier(value);
            filter = new QualifierFilter(op, new BinaryComparator(Bytes.toBytes(value)));
        } else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(key)){
            filter = new RowFilter(op, new BinaryComparator(Bytes.toBytes(value)));
        } else if (HBaseSqlContants.COLUMN_FAMILY.equalsIgnoreCase(key)) {
            filter = new FamilyFilter(op, new BinaryComparator(Bytes.toBytes(value)));
        }

        if (filter != null) {
            filterList.addFilter(filter);
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        String left = greaterThan.getLeftExpression().toString();
        String right = greaterThan.getRightExpression().toString();

        if(Strings.isNullOrEmpty(left) || Strings.isNullOrEmpty(right)) {
            return;
        }

        // fix bug 4 jsqlpraser
        right = ExpressionUtil.convert(right);

        CompareFilter.CompareOp op = CompareFilter.CompareOp.GREATER;
        Filter filter = null;
        if (HBaseSqlContants.HBASE_COLUMN.equalsIgnoreCase(left)) {
            right = ExpressionUtil.convertColumnQualifier(right);
            filter = new QualifierFilter(op, new BinaryComparator(Bytes.toBytes(right)));
        } else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(left)){
            filter = new RowFilter(op, new BinaryComparator(Bytes.toBytes(right)));
        }

        if (filter != null) {
            filterList.addFilter(filter);
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        String left = greaterThanEquals.getLeftExpression().toString();
        String right = greaterThanEquals.getRightExpression().toString();

        if(Strings.isNullOrEmpty(left) || Strings.isNullOrEmpty(right)) {
            return;
        }

        // fix bug 4 jsqlpraser
        right = ExpressionUtil.convert(right);

        CompareFilter.CompareOp op = CompareFilter.CompareOp.GREATER_OR_EQUAL;
        Filter filter = null;
        if (HBaseSqlContants.HBASE_COLUMN.equalsIgnoreCase(left)) {
            right = ExpressionUtil.convertColumnQualifier(right);
            filter = new QualifierFilter(op, new BinaryComparator(Bytes.toBytes(right)));
        } else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(left)){
            filter = new RowFilter(op, new BinaryComparator(Bytes.toBytes(right)));
        }

        if (filter != null) {
            filterList.addFilter(filter);
        }
    }

    public void visit(InExpression inExpression) {
        String key = inExpression.getLeftExpression().toString();
        ItemsList itemsList = inExpression.getItemsList();
        List<String> values = ExpressionUtil.getStringList(itemsList);

        if(Strings.isNullOrEmpty(key) || CollectionUtils.isEmpty(values)) {
            return;
        }

        List<Filter> filters = new ArrayList<Filter>();
        if (HBaseSqlContants.HBASE_COLUMN.equalsIgnoreCase(key)) {
            for (String value : values) {
                // fix bug 4 jsqlpraser
                value = ExpressionUtil.convert(value);
                value = ExpressionUtil.convertColumnQualifier(value);
                QualifierFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
                filters.add(filter);
            }
        } else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(key)){
            for (String value : values) {
                // fix bug 4 jsqlpraser
                value = ExpressionUtil.convert(value);
                RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(value)));
                filters.add(filter);
            }
        }

        if (CollectionUtils.isNotEmpty(filters)) {
            FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
            filterList.addFilter(list);
        }
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {

    }

    /**
     * 暂时支持根据rowkey,column前缀过滤(只支持单个rowKey like + 单个column like)
     * @param likeExpression
     */
    public void visit(LikeExpression likeExpression) {
        String left = likeExpression.getLeftExpression().toString();
        String right = likeExpression.getRightExpression().toString();
        if(Strings.isNullOrEmpty(left) || Strings.isNullOrEmpty(right)) {
            return;
        }

        // fix bug 4 jsqlpraser
        right = ExpressionUtil.convert(right);

        Filter filter = null;
        if (HBaseSqlContants.HBASE_COLUMN.equalsIgnoreCase(left)) {
            right = ExpressionUtil.convertColumnQualifier(right);
            filter = new ColumnPrefixFilter(Bytes.toBytes(right));
        } else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(left)){
            filter = new PrefixFilter(Bytes.toBytes(right));
        }
        filterList.addFilter(filter);
    }

    @Override
    public void visit(MinorThan minorThan) {
        String left = minorThan.getLeftExpression().toString();
        String right = minorThan.getRightExpression().toString();

        if(Strings.isNullOrEmpty(left) || Strings.isNullOrEmpty(right)) {
            return;
        }

        // fix bug 4 jsqlpraser
        right = ExpressionUtil.convert(right);

        CompareFilter.CompareOp op = CompareFilter.CompareOp.LESS;
        Filter filter = null;
        if (HBaseSqlContants.HBASE_COLUMN.equalsIgnoreCase(left)) {
            right = ExpressionUtil.convertColumnQualifier(right);
            filter = new QualifierFilter(op, new BinaryComparator(Bytes.toBytes(right)));
        } else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(left)){
            filter = new RowFilter(op, new BinaryComparator(Bytes.toBytes(right)));
        }

        if (filter != null) {
            filterList.addFilter(filter);
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        String left = minorThanEquals.getLeftExpression().toString();
        String right = minorThanEquals.getRightExpression().toString();

        if(Strings.isNullOrEmpty(left) || Strings.isNullOrEmpty(right)) {
            return;
        }

        // fix bug 4 jsqlpraser
        right = ExpressionUtil.convert(right);

        CompareFilter.CompareOp op = CompareFilter.CompareOp.LESS_OR_EQUAL;
        Filter filter = null;
        if (HBaseSqlContants.HBASE_COLUMN.equalsIgnoreCase(left)) {
            right = ExpressionUtil.convertColumnQualifier(right);
            filter = new QualifierFilter(op, new BinaryComparator(Bytes.toBytes(right)));
        } else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(left)){
            filter = new RowFilter(op, new BinaryComparator(Bytes.toBytes(right)));
        }

        if (filter != null) {
            filterList.addFilter(filter);
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {

    }

    @Override
    public void visit(Column column) {

    }

    @Override
    public void visit(CaseExpression caseExpression) {

    }

    @Override
    public void visit(WhenClause whenClause) {

    }

    @Override
    public void visit(ExistsExpression existsExpression) {

    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {

    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {

    }

    @Override
    public void visit(Concat concat) {

    }

    @Override
    public void visit(Matches matches) {

    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {

    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {

    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {

    }

    public void visit(Table table) {
        if (table != null && !Strings.isNullOrEmpty(table.getWholeTableName())) {
            // fix bug 4 jsqlpraser
            tableNames.add(ExpressionUtil.convert(table.getWholeTableName()));
        }
    }

    @Override
    public void visit(SubSelect subSelect) {

    }

    @Override
    public void visit(SubJoin subJoin) {

    }

    /**
     * 设置column
     *
     * @param selectItems
     */
    private void setQueryColumns(List<SelectItem> selectItems) {
        if (selectItems == null || selectItems.size() <= 0) {
            return;
        }
        for (SelectItem item : selectItems) {
            String colStr = item.toString();
            if (Strings.isNullOrEmpty(colStr)) {
                continue;
            }
            // fix bug 4 jsqlpraser
            colStr = ExpressionUtil.convert(colStr);
            String[] columnGroup = colStr.split(HBaseSqlContants.POINT);
            if (columnGroup != null && columnGroup.length == 2) {
                String columnFamily = columnGroup[0];
                String column = columnGroup[1];
                if (Strings.isNullOrEmpty(columnFamily) || Strings.isNullOrEmpty(column)) {
                    List<String> columns = queryColumnMap.get(columnGroup[0]);
                    if (columns == null) {
                        columns = new ArrayList<String>();
                    }
                    columns.add(column);
                    queryColumnMap.put(columnFamily, columns);

                    scanner.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                }
            } else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(colStr)) {

            } else if (HBaseSqlContants.ASTERISK.equalsIgnoreCase(colStr)) {

            }
            queryColumns.add(colStr);
        }
    }

    public void visit(PlainSelect plainSelect) {
        if (plainSelect == null) {
            return;
        }
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        setQueryColumns(selectItems);

        Limit limit = plainSelect.getLimit();

        if (limit != null) {
            offset = limit.getOffset();
            rowCount = limit.getRowCount();
        }

        plainSelect.getFromItem().accept(this);
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }
    }

    @Override
    public void visit(Union union) {

    }

}
