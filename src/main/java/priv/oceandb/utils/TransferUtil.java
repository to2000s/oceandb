package priv.oceandb.utils;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import priv.oceandb.dao.IdDao;
import priv.oceandb.model.DataPoint;

import java.io.IOException;

/**
 * 转换工具类
 * 提供 data point 与 hbase 存储内容的转换
 */
@Component
public class TransferUtil {

    @Autowired
    EncodeUtil encodeUtil;
    @Autowired
    DecodeUtil decodeUtil;
    @Autowired
    IdDao idDao;
    @Autowired
    CacheUtil cacheUtil;

    public byte[][] trans2byte(DataPoint dataPoint) throws IOException {
        // 数据点各部分
        String param = dataPoint.getParam();
        long timestamp = dataPoint.getTimestamp();
        double lat = dataPoint.getLat();
        double lng = dataPoint.getLng();
        String[] props = dataPoint.getProps();
        double v = dataPoint.getValue();

        // 先读缓存，再读数据库
        byte[] paramId = cacheUtil.getId(param, 0, 0);
        byte[][] propsId = new byte[props.length][];
        for (int i = 0; i < props.length; i++) {
            // 1 2 1 2
            propsId[i] = cacheUtil.getId(props[i], 0, i / 2 + 1);
        }

        // 存入HBase的各部分
        byte[] rowkey = encodeUtil.getRowKey(paramId, timestamp, lat, lng, propsId);
        byte[] qualifier = encodeUtil.getQualifier(timestamp);
        byte[] value = encodeUtil.getValue(lat, lng, v);

        byte[][] result =  {rowkey, qualifier, value};
        return result;
    }

    public DataPoint trans2data(byte[] rowkey, byte[] qualifier, byte[] value) throws IOException {
        byte[] paramId = decodeUtil.getParamId(rowkey);
        long timestamp = decodeUtil.getTimestamp(rowkey, qualifier);
        double[] latLng = decodeUtil.getLatLng(rowkey, value);
        byte[] propsId = decodeUtil.getProps(rowkey);
        double v = decodeUtil.getValue(value);

        // 将 param props 的 id 转换为 name
        String param = cacheUtil.getName(paramId, 1, 0);
        String[] props = new String[propsId.length / 3];
        for (int i = 0; i < propsId.length / 3; i++) {
            // copy(byte[] b, int off, int len)   0 1 2 | 3
            byte[] temp = Bytes.copy(propsId, i * 3, 3);
            // 1 2 1 2
            props[i] = cacheUtil.getName(temp, 1, i / 2 + 1);
        }

        return new DataPoint(param, timestamp, latLng[0], latLng[1], props, v);
    }
}
