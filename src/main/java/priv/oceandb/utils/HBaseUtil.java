package priv.oceandb.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.util.List;

/**
 * HBase操作工具类
 * 单例
 * 用注解可以吗？有 Admin Connection这两个变量
 */
public class HBaseUtil {

    // 单例，初始化即创建
    private static HBaseUtil hBaseUtil = new HBaseUtil();

    private HBaseUtil() {}

    public static HBaseUtil getInstance() {
        return hBaseUtil;
    }

    // HBase连接初始化
    static {
        try {
            hBaseUtil.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Autowired
    private Environment environment;
    private Connection connection;   // 连接对象
    private Admin admin;             // 元数据操作对象

    /**
     * HBase连接初始化
     */
    private void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        // 配置有些问题，environment是空的
//        conf.set("hbase.zookeeper.quorum", environment.getProperty("hbase.zookeeper_quorum"));
//        conf.set("hbase.zookeeper.property.clientPort", environment.getProperty("hbase.zookeeper_property.clientPort"));
//        conf.set("zookeeper.znode.parent", environment.getProperty("hbase.zookeeper_znode_parent"));
//        conf.set("hbase.client.start.log.errors.counter", environment.getProperty("hbase.client_start_log_errors_counter"));
//        conf.set("hbase.client.retries.number", environment.getProperty("hbase.client_retries_number"));

        conf.set("hbase.zookeeper.quorum", "master2.oceancloud,master0.oceancloud,master1.oceancloud");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.client.start.log.errors.counter", "1");
        conf.set("hbase.client.retries.number", "1");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    /**
     * 创建表，输入表名、列族名
     */
    public void createTable(String tableName, String[] columnFamilies) throws IOException {
        if (! admin.tableExists(TableName.valueOf(tableName))) {
            // 表描述对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

            for (int i = 0; i < columnFamilies.length; i++) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamilies[i]);
                hTableDescriptor.addFamily((hColumnDescriptor));
            }

            try {
                admin.createTable(hTableDescriptor);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 删除表，输入表名
     */
    public void dropTable(String tableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
    }

    /**
     * 通过表名、行键查询一行数据
     */
    public Result query(String tableName, byte[] rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 查询对象 条件
        Get get = new Get(rowkey);

        // result 一行
        return table.get(get);
    }

    /**
     * 通过表名、行键、限定符、值插入数据
     */
    public void insert(String tableName, byte[] rowkey, byte[] cf, byte[] qualifier, byte[] value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(rowkey);
        put.addColumn(cf, qualifier, value);
        table.put(put);
    }

    /**
     * 按照filter要求进行scan
     */
    public ResultScanner scan(String tableName, List<Filter> filters) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setFilter(new FilterList(filters));

        return table.getScanner(scan);
    }

    /**
     * 按照filter要求进行scan
     */
    public ResultScanner scan(String tableName, List<Filter> passAllFilters, List<Filter> passOnefilters) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setFilter(new FilterList(passAllFilters));
        scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, passOnefilters));

        return table.getScanner(scan);
    }
}
