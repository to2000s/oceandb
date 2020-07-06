package wanglinxin.online.oceandb.dao;

import org.apache.hadoop.hbase.util.Bytes;
import wanglinxin.online.oceandb.utils.HBaseUtil;

import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * 对id表的操作
 */
public class IdDao {

    /**
     * 创建id表
     */
    public void createTable() throws IOException {
        HBaseUtil.getInstance().createTable("id", new String[] {"name", "id"});
    }

    /**
     * 删除id表
     */
    public void dropTable() throws IOException {
        HBaseUtil.getInstance().dropTable("id");
    }

    /**
     * 获取name对应的id，若无，新建双向映射
     */
    public byte[] getId(String name) throws IOException {
        Result result = HBaseUtil.getInstance().queryTable("id", Bytes.toBytes(name));
        if (result == null) {
            return createId(name);
        } else {
            // 从result通过限定符获取数据
        }

        return new byte[1];
    }

    /**
     * 获取id对应的name
     */
    public String getName(byte[] id) throws IOException {
        Result result = HBaseUtil.getInstance().queryTable("id", id);
        // 获取数据

        return "";
    }

    /**
     * 创建映射的方法
     * 需要匹配一个未用过的id
     * 用什么算法可以尽可能随机分散？
     * 找中间点，搜索判定是否用过
     */
    private byte[] createId(String name) {
        // 向id表插入数据
        return new byte[1];
    }

}
