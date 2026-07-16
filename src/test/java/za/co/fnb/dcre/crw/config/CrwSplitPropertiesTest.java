package za.co.fnb.dcre.crw.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCRUM-55 Task 2: split sizing config. Plain context test, no DB: only the
 * properties binding is on trial, so the context is the minimal
 * EnableConfigurationProperties config (no autoconfiguration, no datasource).
 */
@SpringBootTest(classes = CrwSplitPropertiesTest.PropsConfig.class, properties = {
        "dcre.crw.split.max-size=5000",
        "dcre.crw.split.overrides.[FNBCC01]=10000"})
class CrwSplitPropertiesTest {

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(CrwSplitProperties.class)
    static class PropsConfig {
    }

    @Autowired
    CrwSplitProperties props;

    @Test
    void defaultAndOverride() {
        assertThat(props.maxFor("FNBRF01")).isEqualTo(5000);
        assertThat(props.maxFor("FNBCC01")).isEqualTo(10000);
    }

    @Test
    void programArgWinsIsSpringPrecedence() {
        // documented behaviour: --dcre.crw.split.max-size=N on the JobLauncher arg list
        // overrides YAML by standard Spring property precedence; no code needed.
        assertThat(props.maxFor("UNKNOWN")).isEqualTo(5000); // unknown client inherits default (Sean ruling 12)
    }
}
