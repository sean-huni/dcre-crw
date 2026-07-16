package za.co.fnb.dcre.crw.config;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.crw.service.EmissionTasklet;
import za.co.fnb.dcre.platform.batch.CrdbRetryExceptionHandler;
import za.co.fnb.dcre.platform.batch.OutcomeFileWriter;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;

import javax.sql.DataSource;
import java.nio.file.Path;

@Configuration
@EnableConfigurationProperties(CrwSplitProperties.class)
public class CrwJobConfig {

    @Bean
    public Job crwJob(JobRepository repo, PlatformTransactionManager tx, EmissionTasklet tasklet,
                      @Value("${dcre.exchange-root}") String exchangeRoot) {
        // CRDB 40001 aborts hit the tasklet's commit boundary under contention; the shared
        // platform handler re-runs the WHOLE tasklet in a fresh tx, which is safe here by
        // design: claim-once snapshot (R-24), member ON CONFLICT no-ops, StagedWrite
        // restart no-op (R-05) and the VISIBLE guard. Retry, never skip.
        Step emitStep = new StepBuilder("emitStep", repo).tasklet(tasklet, tx)
                .exceptionHandler(new CrdbRetryExceptionHandler("CRW")).build();
        return new JobBuilder("crwJob", repo)
                .listener(new SeamListener(exchangeRoot))
                .start(emitStep)
                .build();
    }

    @Bean
    @Order(-10)
    public ApplicationRunner staleExecutionSweep(DataSource dataSource) {
        return args -> StaleExecutionSweeper.abandonStale(dataSource, "CRW_BATCH_", 60);
    }

    record SeamListener(String exchangeRoot) implements JobExecutionListener {

        @Override
        public void afterJob(JobExecution execution) {
            if (execution.getStatus() != BatchStatus.COMPLETED) {
                return;
            }
            String jobName = System.getenv().getOrDefault("JOB_NAME", "local-" + execution.getId());
            OutcomeFileWriter.write(Path.of(exchangeRoot), jobName, "BUSINESS_ACCEPTED");
        }
    }
}
