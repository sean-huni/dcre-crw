package za.co.fnb.dcre.crw.config;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.StepLocator;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.transaction.support.DefaultTransactionStatus;
import za.co.fnb.dcre.crw.service.EmissionService;
import za.co.fnb.dcre.crw.service.EmissionTasklet;

import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Registration proof for the shared CRDB 40001 retry handler on the
 * PRODUCTION emitStep (platform-batch CrdbRetryExceptionHandler): the only
 * injected failure is thrown from PlatformTransactionManager.doCommit, the
 * observed live failure mode. An emitStep without the handler fails on the
 * first abort; a wired one re-runs the whole tasklet in a fresh transaction,
 * which the EmissionService idempotency design (R-24 claim-once, R-05
 * StagedWrite no-op, VISIBLE guard) makes safe.
 */
class CrwJobConfigRetryTest {

    /** Fails the first {@code failures} commits the way JdbcTransactionManager surfaces a CRDB 40001. */
    static final class CommitFailingTxManager extends ResourcelessTransactionManager {

        private final int failures;
        private int commits;

        CommitFailingTxManager(final int failures) {
            this.failures = failures;
        }

        @Override
        protected void doCommit(final DefaultTransactionStatus status) {
            if (++commits <= failures) {
                throw new CannotAcquireLockException(
                        "JDBC commit; ERROR: restart transaction: TransactionRetryWithProtoRefreshError:"
                                + " RETRY_ASYNC_WRITE_FAILURE");
            }
            super.doCommit(status);
        }
    }

    @Test
    void emitStepRetriesCommitTimeCrdbAbortsThenCompletes() throws Exception {
        final var emissions = new AtomicInteger();
        final EmissionService countingService = new EmissionService(null, null, null, null, null,
                new ResourcelessTransactionManager()) {
            @Override
            public int emitDue(final LocalDate runDate) {
                emissions.incrementAndGet();
                return 0;
            }
        };

        final var repo = new ResourcelessJobRepository();
        final Job job = new CrwJobConfig().crwJob(repo, new CommitFailingTxManager(2),
                new EmissionTasklet(countingService), "build/test-exchange");
        final Step emitStep = ((StepLocator) job).getStep("emitStep");

        final JobParameters params = new JobParametersBuilder()
                .addString("run.date", "2026-07-14", true).toJobParameters();
        final JobInstance instance = repo.createJobInstance("crwJob", params);
        final JobExecution jobExecution = repo.createJobExecution(instance, params, new ExecutionContext());
        final StepExecution stepExecution = repo.createStepExecution("emitStep", jobExecution);
        emitStep.execute(stepExecution);

        assertEquals(BatchStatus.COMPLETED, stepExecution.getStatus(),
                "two commit-time 40001 aborts must be retried on emitStep, not fail it");
        assertEquals(3, emissions.get(), "whole tasklet re-runs in a fresh transaction per aborted commit");
    }
}
