package priv.oceandb.dao;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;
import priv.oceandb.utils.HBaseUtil;

import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Random;

/**
 * 对id表的操作
 */
@Repository
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
     * param prop_key prop_value 是否会出现同名的情况？
     * id也有可能与name相同，比如prop_value是数字的情况!
     * 因此有必要设计成论文中的形式
     * 0 1 2 指定 qualifier
     */
    public byte[] getId(String name, int qualifer) throws IOException {
        Result result = HBaseUtil.getInstance().query("id", Bytes.toBytes(name));
        // 结果集为空，建立映射
        if (result.isEmpty()) {
            return createId(name, qualifer);
        } else {
            byte[] id = result.getValue(Bytes.toBytes("name"),Bytes.tail(Bytes.toBytes(qualifer), 1));
            return id == null ? createId(name, qualifer) : id;
        }
    }

    /**
     * 获取id对应的name
     */
    public String getName(byte[] id, int qualifer) throws IOException {
        Result result = HBaseUtil.getInstance().query("id", id);
        if (result.isEmpty()) {
            return null;
        }
        byte[] name = result.getValue(Bytes.toBytes("id"),Bytes.tail(Bytes.toBytes(qualifer), 1));
        return Bytes.toString(name);
    }

    /**
     * 创建映射的方法
     * 需要匹配一个未用过的id
     * 用什么算法可以尽可能随机分散？直接用随机数算了
     */
    private byte[] createId(String name, int qualifer) throws IOException {

        byte[] id = Bytes.tail(
                Bytes.toBytes(new Random().nextInt()), 3
        );
        // 查询 id 是否已使用
        while (getName(id, qualifer) != null) {
            id = Bytes.tail(
                    Bytes.toBytes(new Random().nextInt()), 3
            );
        }

        // 建立映射
        HBaseUtil.getInstance().insert(
                "id",
                Bytes.toBytes(name),
                Bytes.toBytes("id"),
                Bytes.tail(Bytes.toBytes(qualifer), 1),
                id);
        HBaseUtil.getInstance().insert(
                "id",
                id,
                Bytes.toBytes("name"),
                Bytes.tail(Bytes.toBytes(qualifer), 1),
                Bytes.toBytes(name));
        return id;
    }

}
