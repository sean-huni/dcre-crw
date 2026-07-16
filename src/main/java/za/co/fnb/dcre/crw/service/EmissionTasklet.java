package za.co.fnb.dcre.crw.service;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

import java.time.LocalDate;
import java.util.List;

/**
 * Thin per-lane entry adapter (3-tier, SCRUM-55 Feature 2): one partitioned
 * worker execution = one client lane (comma-joined client token from the
 * ClientLanePartitioner). Job identity: (run.date, window) per R-16/R-37.
 * Instantiated step-scoped by CrwJobConfig, one instance per lane.
 */
public class EmissionTasklet implements Tasklet {

    private final LaneEmissionService lanes;
    private final LocalDate runDate;
    private final List<String> clients;

    public EmissionTasklet(final LaneEmissionService lanes, final LocalDate runDate, final String clientLane) {
        this.lanes = lanes;
        this.runDate = runDate;
        this.clients = List.of(clientLane.split(","));
    }

    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) {
        final int emitted = lanes.emitLane(runDate, clients);
        // per-lane count in the WORKER's own context: parallel lanes must not
        // read-modify-write a shared job-level key.
        chunkContext.getStepContext().getStepExecution().getExecutionContext().putInt("emitted", emitted);
        return RepeatStatus.FINISHED;
    }
}
