package priv.oceandb.utils;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

/**
 * 编码工具类
 * 单例
 */
@Component
public class EncodeUtil {

    /* 时间相关 */

    /**
     * 获取时间戳中的小时部分，Time_base
     */
    public long getTimeBase(long timestamp) {
        return timestamp / 1000 / 3600 * 3600;
    }

    public byte[] getTimeBaseBytes(long timestamp) {
        // 4字节空间，够放100年数据了...
        return Bytes.tail(Bytes.toBytes(getTimeBase(timestamp)), 4);
    }

    /**
     * 获取Time_offset
     */
    public long getTimeOffset(long timestamp) {
        return timestamp - getTimeBase(timestamp) * 1000;
    }

    public byte[] getTimeOffsetBytes(long timestamp) {
        // 精确到毫秒，3600000以内，3字节
        return Bytes.tail(Bytes.toBytes(getTimeOffset(timestamp)), 3);
    }

    /* 空间相关 */

    /**
     * 获取坐标的S2Id
     */
    public S2CellId getS2Id(double lat, double lng) {
        S2LatLng s2LatLng = S2LatLng.fromDegrees(lat, lng);
        S2CellId s2CellId = S2CellId.fromLatLng(s2LatLng);
        return s2CellId;
    }

    /**
     * Area_base存放 level 10 S2Id编码
     */
    public long getAreaBase(double lat, double lng) {
        return getS2Id(lat, lng).parent(10).id();
    }

    /**
     * 从S2Id中提取 level 10 编码的前3字节作为Area_base
     * face 3bit 10*2 标志位1
     */
    public byte[] getAreaBaseBytes(double lat, double lng) {
        return Bytes.head(
                Bytes.toBytes(
                        getAreaBase(lat, lng)
                ), 3
        );
    }

    /**
     * Area_offset精确到 level 26 的 S2Id
     */
    public long getAreaOffset(double lat, double lng) {
        return getS2Id(lat, lng).parent(26).id();
    }

    /**
     * 实际要储存的Area_offset是S2Id的 24-55 bit
     * 前23位bit信息在Area_base中有体现
     * 56位标志位，不需要
     * 长4字节
     */
    public byte[] getAreaOffsetBytes(double lat, double lng) {
        return Bytes.tail(
                Bytes.toBytes(
                        getAreaOffset(lat, lng) >>> 9 & 0xffffffffL
                ), 4
        );
    }

    /* 组合编码 */

    /**
     * 将相关信息组合为RowKey
     */
    public byte[] getRowKey(byte[] paramId, long timestamp, double lat, double lng, byte[][] props) {
        byte[] rowkey = Bytes.add(paramId, getTimeBaseBytes(timestamp), getAreaBaseBytes(lat, lng));
        // 加入属性组
        for (int i = 0; i < props.length; i++) {
            rowkey = Bytes.add(rowkey, props[i]);
        }
        return rowkey;
    }

    /**
     * Time_offset 作为Column Qualifier
     */
    public byte[] getQualifier(long timestamp) {
        return getTimeOffsetBytes(timestamp);
    }

    /**
     * 将Area_offset & value 组合为Value
     */
    public byte[] getValue(double lat, double lng, double value) {
        return Bytes.add(
                getAreaOffsetBytes(lat, lng),
                Bytes.toBytes(value)
        );
    }

}
