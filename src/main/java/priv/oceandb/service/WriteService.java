package priv.oceandb.service;

import org.springframework.web.multipart.MultipartFile;
import priv.oceandb.model.DataPoint;

import java.io.IOException;

public interface WriteService {

    void writeDataPoint(DataPoint dataPoint) throws IOException;

    void writeCsvFile(MultipartFile file);
}
