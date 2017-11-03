package com.hzgc.hbase.consumer;

import com.hzgc.hbase.dynamicrepo.DynamicTable;
import com.hzgc.hbase.util.HBaseHelper;
import com.hzgc.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017-8-22.
 */
public class GetPicFromHBase implements Serializable {
   /* @Test
    public void getPicFromHBase(String rowKey) {
        byte[] image = null;
        if (null != rowKey && rowKey.length() > 0) {
            Table personTable = HBaseHelper.getTable(DynamicTable.TABLE_PERSON);
            Get get = new Get(Bytes.toBytes(rowKey));
            try {
                Result result = personTable.get(get);
                image = result.getValue(DynamicTable.PERSON_COLUMNFAMILY, DynamicTable.PERSON_COLUMN_IMGE);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                HBaseUtil.closTable(personTable);
            }
        }
        return image;
    }
*/
    @Test
    public void getSmallPicFromHbase() {
        Table personTable = HBaseHelper.getTable(DynamicTable.TABLE_PERSON);
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*01"));
        Scan scan = new Scan();
        List<String> rowKeyList = new ArrayList<>();
        scan.setFilter(filter);
        try {
            ResultScanner scanner = personTable.getScanner(scan);
            for (Result result : scanner) {
                byte[] bytes = result.getRow();
                String string = Bytes.toString(bytes);
                rowKeyList.add(string);
            }
            System.out.println(rowKeyList.size());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.closTable(personTable);
        }
    }
}
