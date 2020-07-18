package priv.oceandb.utils;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;
import priv.oceandb.model.IdMapKey;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存工具类，存放id name映射
 * 单例
 * 线程安全HashMap
 */
@Component
public class CacheUtil extends ConcurrentHashMap<IdMapKey, byte[]> {

    public byte[] getId(String name, int family, int qualifier) {
        return get(new IdMapKey(Bytes.toBytes(name), family, qualifier));
    }

    public String getName(byte[] id, int family, int qualifier) {
        // toString(null) -> null
        return Bytes.toString(get(new IdMapKey(id, family, qualifier)));
    }

}
