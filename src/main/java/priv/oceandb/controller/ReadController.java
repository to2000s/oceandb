package priv.oceandb.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import priv.oceandb.model.DataPoint;
import priv.oceandb.service.ReadService;

@RestController
@RequestMapping("/read")
public class ReadController {

    @Autowired
    ReadService readService;

    @GetMapping("/query")
    public DataPoint query(String param, long timestamp, double lat, double lng, String[] props) {
        return readService.query(param, timestamp, lat, lng, props);
    }

    @GetMapping("scan")
    public DataPoint[] scan(String param, long[] timestamps, @Nullable double[] latLng, @Nullable String[] props) {
        // TODO 研究一下传入参数为空的情况
        return readService.scan(param, timestamps, latLng, props);
    }

}
