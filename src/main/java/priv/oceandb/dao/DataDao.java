package priv.oceandb.dao;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import priv.oceandb.model.DataPoint;
import priv.oceandb.utils.HBaseUtil;
import priv.oceandb.utils.TransferUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 对data表的操作
 */
@Repository
public class DataDao {

    @Autowired
    TransferUtil transferUtil;

    /**
     * 创建data表
     */
    public void createTable() throws IOException {
        HBaseUtil.getInstance().createTable("oceandb_data", new String[] {"data"});
    }

    /**
     * 删除data表
     */
    public void dropTable() throws IOException {
        HBaseUtil.getInstance().dropTable("oceandb_data");
    }

    /**
     * 通过rowkey qualifier 查询一条数据
     */
    public byte[] query(byte[] rowkey, byte[] qualifier) throws IOException {
        Result result =  HBaseUtil.getInstance().query("oceandb_data", rowkey);
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
        HBaseUtil.getInstance().insert("oceandb_data", rowkey, Bytes.toBytes("data"), qualifier, value);
    }

    /**
     * 按filter要求进行scan
     */
    public List<DataPoint> scan(List<Filter> filters) throws IOException {
        ResultScanner results = HBaseUtil.getInstance().scan("oceandb_data", filters);
        if (results == null) {
            return null;
        } else {
            List<DataPoint> dataPoints = new ArrayList<>();
            for (Result result : results) {
                byte[] rowkey = result.getRow();

                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("data"));
                for(Map.Entry<byte[], byte[]> entry:familyMap.entrySet()){
                    byte[] qualifier = entry.getKey();
                    byte[] value = entry.getValue();
                    dataPoints.add(transferUtil.trans2data(rowkey, qualifier, value));
                }
            }
//            return dataPoints.toArray(new DataPoint[dataPoints.size()]);
            return dataPoints;  // 集合更好使，否则之后再次筛选又要创建新数组
        }
    }






    /* ... */
}
