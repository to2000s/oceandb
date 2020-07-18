package priv.oceandb.service.impl;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import priv.oceandb.model.DataPoint;
import priv.oceandb.service.ReadService;
import priv.oceandb.utils.CacheUtil;
import priv.oceandb.utils.DecodeUtil;
import priv.oceandb.utils.EncodeUtil;
import priv.oceandb.dao.DataDao;
import priv.oceandb.dao.IdDao;
import priv.oceandb.utils.TransferUtil;

import java.io.IOException;

@Service
public class ReadServiceImpl implements ReadService {

    @Autowired
    EncodeUtil encodeUtil;
    @Autowired
    DecodeUtil decodeUtil;
    @Autowired
    TransferUtil transferUtil;
    @Autowired
    DataDao dataDao;
    @Autowired
    IdDao idDao;
    @Autowired
    CacheUtil cacheUtil;

    @Override
    public DataPoint query(String param, long timestamp, double lat, double lng, String[] props) throws IOException {

        byte[][] result = transferUtil.trans2byte(new DataPoint(param, timestamp, lat, lng, props, 0));
        // rowkey, qualifier
        byte[] value = dataDao.query(result[0], result[1]);
        return value == null ? null : new DataPoint(param, timestamp, lat, lng, props, decodeUtil.getValue(value));
    }

    @Override
    public DataPoint[] scan(String param, long[] timestamp) throws IOException {

        byte[] paramId = cacheUtil.getId(param, 0, 0) == null ?
                idDao.getId(param, 0) : cacheUtil.getId(param, 0, 0);
        byte[] start = Bytes.add(paramId, encodeUtil.getTimeBaseBytes(timestamp[0]));
        byte[] end = Bytes.add(paramId, encodeUtil.getTimeBaseBytes(timestamp[1]));


        return new DataPoint[0];
    }

    @Override
    public DataPoint[] scan(String param, double[] latLng) {
        return new DataPoint[0];
    }
}
