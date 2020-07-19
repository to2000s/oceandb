package priv.oceandb.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 请求参数模型
 * param
 * 时间范围
 * 空间范围 端点逆时针lat lng坐标
 * 属性对
 */
@Data
@AllArgsConstructor
public class RequestParams {
    private String param;
    private long[] period;
    private double[] area;
    private String[] props;
}
