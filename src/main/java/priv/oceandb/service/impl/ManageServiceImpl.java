package priv.oceandb.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import priv.oceandb.dao.DataDao;
import priv.oceandb.dao.IdDao;
import priv.oceandb.service.ManageService;

import java.io.IOException;

@Service
public class ManageServiceImpl implements ManageService {

    @Autowired
    DataDao dataDao;
    @Autowired
    IdDao idDao;

    @Override
    public void init() throws IOException {
        idDao.dropTable();
        dataDao.dropTable();
        idDao.createTable();
        dataDao.createTable();
    }
}
