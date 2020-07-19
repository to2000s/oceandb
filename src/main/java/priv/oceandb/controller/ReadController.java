package priv.oceandb.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import priv.oceandb.model.DataPoint;
import priv.oceandb.model.RequestParams;
import priv.oceandb.service.ReadService;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/read")
public class ReadController {

    @Autowired
    ReadService readService;

    @PostMapping("/query")
    public DataPoint query(@RequestBody DataPoint params) throws IOException {
        return readService.query(params);
    }

    @PostMapping("scan")
    public List<DataPoint> scan(@RequestBody RequestParams params) throws IOException {
        // 请求参数各部分为空的情况，组合

        if (params.getParam() != null &&
            params.getPeriod() != null &&
            params.getArea() == null &&
            params.getProps() == null) {
            return readService.scan(params.getParam(), params.getPeriod());
        }
        if (params.getParam() != null &&
                params.getPeriod() == null &&
                params.getArea() != null &&
                params.getProps() == null) {
            return readService.scan(params.getParam(), params.getArea());
        }

        return null;
    }

}
