package wanglinxin.online.oceandb.dao;

import wanglinxin.online.oceandb.utils.HBaseUtil;

import java.io.IOException;

/**
 * 对data表的操作
 */
public class DataDao {

    /**
     * 创建data表
     */
    public void createTable() throws IOException {
        HBaseUtil.getInstance().createTable("data", new String[] {"name", "id"});

    }

    /**
     * 删除data表
     */
    public void dropTable() throws IOException {
        HBaseUtil.getInstance().dropTable("data");
    }

    /**
     * 通过rowkey获取一行数据
     */



    /* ... */
}
