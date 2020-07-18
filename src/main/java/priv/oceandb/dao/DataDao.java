package priv.oceandb.dao;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;
import priv.oceandb.utils.HBaseUtil;

import java.io.IOException;

/**
 * 对data表的操作
 */
@Repository
public class DataDao {

    /**
     * 创建data表
     */
    public void createTable() throws IOException {
        HBaseUtil.getInstance().createTable("data", new String[] {"data"});
    }

    /**
     * 删除data表
     */
    public void dropTable() throws IOException {
        HBaseUtil.getInstance().dropTable("data");
    }

    /**
     * 通过rowkey qualifier 查询一条数据
     */
    public byte[] query(byte[] rowkey, byte[] qualifier) throws IOException {
        Result result =  HBaseUtil.getInstance().query("data", rowkey);
        if (result.isEmpty()) {
            return null;
        } else {
            byte[] value = result.getValue(Bytes.toBytes("data"), qualifier);
            return value;   // 可以为空
        }
    }

    /**
     * 通过rowkey qualifier value 插入一行数据
     */
    public void insert(byte[] rowkey, byte[] qualifier, byte[] value) throws IOException {
        HBaseUtil.getInstance().insert("data", rowkey, Bytes.toBytes("data"), qualifier, value);
    }

    // TODO scan方法，各种情况下的查询






    /* ... */
}
