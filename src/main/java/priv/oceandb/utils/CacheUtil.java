package priv.oceandb.utils;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import priv.oceandb.dao.IdDao;
import priv.oceandb.model.IdMapKey;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存工具类，存放id name映射
 * 单例
 * 线程安全HashMap
 */
@Component
public class CacheUtil extends ConcurrentHashMap<IdMapKey, byte[]> {

    @Autowired
    IdDao idDao;

    public byte[] getId(String name, int family, int qualifier) throws IOException {
        if (get(new IdMapKey(Bytes.toBytes(name), family, qualifier)) == null) {
            // 缓存中无，idDao查询，查询结果存入缓存
            byte[] id = idDao.getId(name, qualifier);
            put(new IdMapKey(Bytes.toBytes(name), family, qualifier), id);
            return id;
        } else {
            return get(new IdMapKey(Bytes.toBytes(name), family, qualifier));
        }
    }

    public String getName(byte[] id, int family, int qualifier) throws IOException {
        // toString(null) -> null
        if (get(new IdMapKey(id, family, qualifier)) == null) {
            // 缓存中无，idDao查询，查询结果存入缓存
            String name = idDao.getName(id, qualifier);
            put(new IdMapKey(id, family, qualifier), Bytes.toBytes(name));
            return name;
        } else {
            return Bytes.toString(get(new IdMapKey(id, family, qualifier)));
        }
    }

}
