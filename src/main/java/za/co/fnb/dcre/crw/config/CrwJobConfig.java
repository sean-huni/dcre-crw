package za.co.fnb.dcre.crw.config;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
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
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.crw.batch.ClientLanePartitioner;
import za.co.fnb.dcre.crw.service.EmissionService;
import za.co.fnb.dcre.crw.service.EmissionTasklet;
import za.co.fnb.dcre.crw.service.LaneEmissionService;
import za.co.fnb.dcre.platform.batch.CrdbRetryExceptionHandler;
import za.co.fnb.dcre.platform.batch.OutcomeFileWriter;
import za.co.fnb.dcre.platform.batch.PartitionSizer;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.LocalDate;

/**
 * SCRUM-55 Feature 2 job shape: emitStep partitions the run into per-client
 * lanes (CTV validationStep wiring: PartitionSizer + virtual threads); each
 * lane worker emits its clients serially, parents in insertion-order FIFO.
 */
@Configuration
@EnableConfigurationProperties(CrwSplitProperties.class)
public class CrwJobConfig {

    @Bean
    @JobScope
    public ClientLanePartitioner clientLanePartitioner(EmissionService service,
            @Value("#{jobParameters['run.date']}") String runDate) {
        return new ClientLanePartitioner(service, LocalDate.parse(runDate));
    }

    @Bean
    @StepScope
    public EmissionTasklet emissionTasklet(LaneEmissionService lanes,
            @Value("#{jobParameters['run.date']}") String runDate,
            @Value("#{stepExecutionContext['clients']}") String laneClients) {
        return new EmissionTasklet(lanes, LocalDate.parse(runDate), laneClients);
    }

    @Bean
    public Step emitWorkerStep(JobRepository repo, PlatformTransactionManager tx,
                               EmissionTasklet emissionTasklet) {
        // CRDB 40001 aborts hit the lane tasklet's commit boundary under contention;
        // the shared platform handler re-runs the WHOLE tasklet in a fresh tx, which
        // is safe here by design: claim-once snapshot (R-24), member ON CONFLICT
        // no-ops, StagedWrite restart no-op (R-05) and the VISIBLE guard. Retry,
        // never skip. WORKER step only; the partitioned manager step stays without it.
        return new StepBuilder("emitWorkerStep", repo).tasklet(emissionTasklet, tx)
                .exceptionHandler(new CrdbRetryExceptionHandler("CRW")).build();
    }

    @Bean
    public Step emitStep(JobRepository repo, Step emitWorkerStep,
                         ClientLanePartitioner clientLanePartitioner,
                         @Value("${dcre.crw.max-partitions:5}") int maxPartitions) {
        return new StepBuilder("emitStep", repo)
                .partitioner("emitWorkerStep", clientLanePartitioner)
                .step(emitWorkerStep)
                .gridSize(PartitionSizer.partitions(maxPartitions))
                .taskExecutor(new VirtualThreadTaskExecutor("crw-lane-"))
                .build();
    }

    @Bean
    public Job crwJob(JobRepository repo, Step emitStep, @Value("${dcre.exchange-root}") String exchangeRoot) {
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
