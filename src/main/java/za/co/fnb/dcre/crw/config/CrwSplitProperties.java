package za.co.fnb.dcre.crw.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * SCRUM-55: max tx per outbound pain.008. Unknown clients inherit the default
 * (Sean ruling 12); a JobLauncher program arg --dcre.crw.split.max-size=N wins
 * by standard Spring property precedence. INTERIM home until the R-14 client
 * reference table materializes (R-39).
 */
@ConfigurationProperties(prefix = "dcre.crw.split")
public record CrwSplitProperties(int maxSize, Map<String, Integer> overrides) {

    public CrwSplitProperties {
        overrides = overrides == null ? Map.of() : overrides;
        if (maxSize < 1) {
            throw new IllegalArgumentException("dcre.crw.split.max-size must be >= 1");
        }
    }

    public int maxFor(final String client) {
        return overrides.getOrDefault(client, maxSize);
    }
}
