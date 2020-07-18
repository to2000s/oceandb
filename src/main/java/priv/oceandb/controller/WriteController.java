package priv.oceandb.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import priv.oceandb.model.DataPoint;
import priv.oceandb.service.WriteService;

import java.io.IOException;

/**
 * 控制器类，写入数据
 */
@RestController
@RequestMapping("/write")
public class WriteController {

    @Autowired
    WriteService writeService;

    @PostMapping("/dataPoint")
    public void writeDataPoint(@RequestBody DataPoint dataPoint) throws IOException {
        writeService.writeDataPoint(dataPoint);
    }

    @PostMapping("/csvFile")
    public void writeCsvFile() {

    }

}
