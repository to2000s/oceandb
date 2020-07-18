package priv.oceandb.utils;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class EncodeAndDecodeTest {

    @Autowired
    EncodeUtil encodeUtil;
    @Autowired
    DecodeUtil decodeUtil;

    @Test
    public void test() {


        // 示例数据
        long time = 1593864104373L;
        double lat = 26.0334;
        double lng = 112.3423;
        byte[] param_id = Bytes.tail(Bytes.toBytes(1), 3);
        byte[] prop_name = Bytes.tail(Bytes.toBytes(1), 3);
        byte[] prop_value = Bytes.tail(Bytes.toBytes(1), 3);
        byte[][] params = {prop_name, prop_value};
        double v = 32.34;

        byte[] rowkey = encodeUtil.getRowKey(param_id, time, lat, lng, params);
        byte[] column = encodeUtil.getQualifier(time);
        byte[] value = encodeUtil.getValue(lat, lng, v);

        System.out.println(Bytes.toLong(Bytes.head(rowkey, 8)));
        System.out.println(Bytes.toLong(Bytes.tail(rowkey, 8)));

        System.out.println("----------------------------------------");

        System.out.println(decodeUtil.getTimestamp(rowkey, column));
        System.out.println(decodeUtil.getLatLng(rowkey, value)[0]);
        System.out.println(decodeUtil.getLatLng(rowkey, value)[1]);
        System.out.println(Bytes.toInt(Bytes.add(new byte[1], decodeUtil.getParamId(rowkey))));
        System.out.println(Bytes.toLong(Bytes.add(new byte[2], decodeUtil.getProps(rowkey))));
        System.out.println(decodeUtil.getValue(value));
    }
}
