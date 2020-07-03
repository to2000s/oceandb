package wanglinxin.online.oceandb.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import java.io.IOException;

/**
 * HBase操作工具类
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
    public Connection connection;   // 连接对象
    public Admin admin;             // 元数据操作对象

    /**
     * HBase连接初始化
     */
    private void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", environment.getProperty("hbase.zookeeper_quorum"));
        conf.set("hbase.zookeeper.property.clientPort", environment.getProperty("hbase.zookeeper_property.clientPort"));
        conf.set("zookeeper.znode.parent", environment.getProperty("hbase.zookeeper_znode_parent"));
        conf.set("hbase.client.start.log.errors.counter", environment.getProperty("hbase.client_start_log_errors_counter"));
        conf.set("hbase.client.retries.number", environment.getProperty("hbase.client_retries_number"));

        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    /**
     * 创建表，输入表名、列族名
     */
    public void createTable(TableName tableName, String[] columnFamilies) throws IOException {
        if (! admin.tableExists(tableName)) {
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
    public void dropTable(String tableNameString) throws IOException {
        admin.disableTable(TableName.valueOf(tableNameString));
        admin.deleteTable(TableName.valueOf(tableNameString));
    }
}
