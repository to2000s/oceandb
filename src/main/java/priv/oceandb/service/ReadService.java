package priv.oceandb.service;

import priv.oceandb.model.DataPoint;

import java.io.IOException;
import java.util.List;

public interface ReadService {

    DataPoint query(DataPoint dataPoint) throws IOException;

    // 各种条件下的scan查询
//    DataPoint[] scan(String param, long[] timestamp, @Nullable double[] latLng, @Nullable String[] props);
    // 根据参数检索
    List<DataPoint> scan(String param) throws IOException;
    // 根据属性检索
    List<DataPoint> scan(String[] props) throws IOException;
    // 根据参数、时间段检索
    List<DataPoint> scan(String param, long[] timestamp) throws IOException;
    // 根据参数、区域范围检索
    List<DataPoint> scan(String param, double[] latLng) throws IOException;
    // 根据参数、时间段、属性检索
    List<DataPoint> scan(String param, long[] timestamp, String[] props) throws IOException;
    // 根据参数、时间段、属性检索
    List<DataPoint> scan(String param, double[] latLng, String[] props) throws IOException;
    // 根据参数、时间段、区域范围、属性检索
    List<DataPoint> scan(String param, long[] timestamp, double[] latLng, String[] props) throws IOException;
}
