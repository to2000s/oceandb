package priv.oceandb.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 数据点模型
 * 参数、时间戳、经度、纬度、属性对
 */
@Data
@AllArgsConstructor
public class DataPoint {

    private String param;
    private long timestamp;
    private double lat;
    private double lng;
    private String[] props;
    private double value;

}
