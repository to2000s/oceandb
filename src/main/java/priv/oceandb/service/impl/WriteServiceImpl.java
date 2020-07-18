package priv.oceandb.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import priv.oceandb.model.DataPoint;
import priv.oceandb.service.WriteService;
import priv.oceandb.utils.CacheUtil;
import priv.oceandb.dao.DataDao;
import priv.oceandb.dao.IdDao;
import priv.oceandb.utils.TransferUtil;

import java.io.IOException;

@Service
public class WriteServiceImpl implements WriteService {

    @Autowired
    TransferUtil transferUtil;
    @Autowired
    CacheUtil cacheUtil;
    @Autowired
    DataDao dataDao;
    @Autowired
    IdDao idDao;

    @Override
    public void writeDataPoint(DataPoint dataPoint) throws IOException {

        byte[][] result = transferUtil.trans2byte(dataPoint);
        // rowkey, qualifier, value
        dataDao.insert(result[0], result[1], result[2]);

    }

    @Override
    public void writeCsvFile(MultipartFile file) {

        //TODO 上传文件，读取文件，写入每一行数据点

    }


}
