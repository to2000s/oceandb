package priv.oceandb.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import priv.oceandb.model.DataPoint;
import priv.oceandb.service.WriteService;
import priv.oceandb.utils.CacheUtil;
import priv.oceandb.utils.DecodeUtil;
import priv.oceandb.utils.EncodeUtil;
import priv.oceandb.dao.DataDao;
import priv.oceandb.dao.IdDao;

import java.io.IOException;

@Service
public class WriteServiceImpl implements WriteService {

    @Autowired
    EncodeUtil encodeUtil;
    @Autowired
    DecodeUtil decodeUtil;
    @Autowired
    CacheUtil cacheUtil;
    @Autowired
    DataDao dataDao;
    @Autowired
    IdDao idDao;

    @Override
    public void writeDataPoint(DataPoint dataPoint) throws IOException {

        // 数据点各部分
        String param = dataPoint.getParam();
        long timestamp = dataPoint.getTimestamp();
        double lat = dataPoint.getLat();
        double lng = dataPoint.getLng();
        String[] props = dataPoint.getProps();
        double v = dataPoint.getValue();

        // 先读缓存，再读数据库
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
        byte[] value = encodeUtil.getValue(lat, lng, v);

        dataDao.insert(rowkey, qualifier, value);

    }

    @Override
    public void writeCsvFile(MultipartFile file) {

        //TODO 上传文件，读取文件，写入每一行数据点

    }
}
