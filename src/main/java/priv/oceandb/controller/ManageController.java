package priv.oceandb.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import priv.oceandb.service.ManageService;

import java.io.IOException;

@RestController
@RequestMapping("/manage")
public class ManageController {

    @Autowired
    ManageService manageService;

    @RequestMapping("/init")
    public void init() throws IOException {
        manageService.init();
    }
}
