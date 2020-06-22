package org.edfi.sis;

import org.edfi.sis.service.SisConnectorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(scanBasePackages = "org.edfi")
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class})
public class SisConnectorApp {
    private static final Logger logger = LoggerFactory.getLogger(SisConnectorApp.class);

    public static void main(String[] args) {
        try {
            SpringApplicationBuilder sb = new SpringApplicationBuilder(SisConnectorApp.class);
            sb.web(WebApplicationType.NONE).run(args);
            ConfigurableApplicationContext c = sb.context();

            SisConnectorService service = c.getBean(SisConnectorService.class);
            service.handleRequest();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}