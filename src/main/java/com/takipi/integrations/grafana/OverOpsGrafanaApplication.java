package com.takipi.integrations.grafana;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication
public class OverOpsGrafanaApplication {

    public static void main(String[] args) {
        SpringApplication.run(OverOpsGrafanaApplication.class, args);
    }
}
