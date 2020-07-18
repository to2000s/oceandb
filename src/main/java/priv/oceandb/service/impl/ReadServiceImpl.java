package priv.oceandb.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import priv.oceandb.model.DataPoint;
import priv.oceandb.service.ReadService;
import priv.oceandb.utils.CacheUtil;
import priv.oceandb.utils.DecodeUtil;
import priv.oceandb.utils.EncodeUtil;
import priv.oceandb.dao.DataDao;
import priv.oceandb.dao.IdDao;

import java.io.IOException;

@Service
public class ReadServiceImpl implements ReadService {

    @Autowired
    EncodeUtil encodeUtil;
    @Autowired
    DecodeUtil decodeUtil;
    @Autowired
    DataDao dataDao;
    @Autowired
    IdDao idDao;
    @Autowired
    CacheUtil cacheUtil;

    @Override
    public DataPoint query(String param, long timestamp, double lat, double lng, String[] props) throws IOException {
        // TODO 转换工作提取到TransferUtil
        byte[] paramId = cacheUtil.getId(param, 0, 0) == null ?
                idDao.getId(param, 0) : cacheUtil.getId(param, 0, 0);
        byte[][] propsId = new byte[props.length][];
        for (int i = 0; i < props.length; i++) {
            // 1 2 1 2
            propsId[i] = cacheUtil.getId(param, 0, i / 2 + 1) == null ?
                    idDao.getId(props[i], i / 2 + 1) : cacheUtil.getId(param, 0, i / 2 + 1);
        }

        // 存入HBase的各部分
        byte[] rowkey = encodeUtil.getRowKey(paramId, timestamp, lat, lng, propsId);
        byte[] qualifier = encodeUtil.getQualifier(timestamp);

        byte[] value = dataDao.query(rowkey, qualifier);
        return value == null ? null : new DataPoint(param, timestamp, lat, lng, props, decodeUtil.getValue(value));
    }

    @Override
    public DataPoint[] scan(String param, long[] timestamps, double[] latLng, String[] props) {

        // 如果条件中有单时间点，单经纬度点怎么搞

        // latLng props 可选
        if (latLng == null && props == null) {

        }

        if (latLng == null) {

        }

        if (props == null) {

        }

        // 所有参数都有


        return new DataPoint[0];
    }
}
