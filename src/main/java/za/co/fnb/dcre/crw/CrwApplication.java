package za.co.fnb.dcre.crw;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import za.co.fnb.dcre.platform.batch.ExitCodeMain;
import za.co.fnb.dcre.platform.persistence.JdbcConfig;

@SpringBootApplication
@Import(JdbcConfig.class)
public class CrwApplication {

    public static void main(String[] args) {
        ExitCodeMain.run(CrwApplication.class, args);
    }
}
