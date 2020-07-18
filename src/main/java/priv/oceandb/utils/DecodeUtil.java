package priv.oceandb.utils;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

/**
 * 解码工具类
 * 单例
 */
@Component
public class DecodeUtil {

    /**
     * 输入rowkey，column
     * 抽取出时间戳，精度为毫秒
     */
    public long getTimestamp(byte[] rowkey, byte[] column) {
        // to转换，后面的bytes数组必须满足长度要求！
        return Bytes.toLong(
                Bytes.add(
                        new byte[4],
                        Bytes.copy(rowkey, 3, 4)
                )
        ) * 1000L + Bytes.toLong(
                Bytes.add(
                        new byte[5],
                        column
                )
        );
    }

    /**
     * 输入rowkey，value
     * 抽取出经纬度
     */
    public double[] getLatLng(byte[] rowkey, byte[] value) {
        long areaBase = Bytes.toLong(
                Bytes.add(
                        new byte[5],
                        Bytes.copy(rowkey, 7, 3)
                ));
        long areaOffset = Bytes.toLong(
                Bytes.add(
                        new byte[4],
                        Bytes.head(value, 4)
                ));

        // 组合为level 26的S2Id，base标志位赋0，与offset首位对齐进行或运算
        long s2Id = ((areaBase & ~1) << 40) | ((areaOffset << 1 | 1) << 8);
        S2CellId s2CellId = new S2CellId(s2Id);
        S2LatLng s2LatLng = s2CellId.toLatLng();

        return new double[] {s2LatLng.latDegrees(), s2LatLng.lngDegrees()};
    }

    /**
     * 输入rowkey
     * 抽取出Param_id
     */
    public byte[] getParamId(byte[] rowkey) {
        return Bytes.head(rowkey, 3);
    }

    /**
     * 输入rowkey
     * 抽取出属性对props
     */
    public byte[] getProps(byte[] rowkey) {
        // 不定长
        return Bytes.copy(rowkey, 10, rowkey.length - 10);
    }

    /**
     * 输入value，得到参数值
     */
    public double getValue(byte[] value) {
        return Bytes.toDouble(Bytes.tail(value, 8));
    }


}
