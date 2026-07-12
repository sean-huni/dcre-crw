package za.co.fnb.dcre.crw.service;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/** Thin entry adapter (3-tier). Job identity: (run.date, window) per R-16/R-37. */
@Component
public class EmissionTasklet implements Tasklet {

    private final EmissionService service;

    public EmissionTasklet(EmissionService service) {
        this.service = service;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        LocalDate runDate = LocalDate.parse(
                (String) chunkContext.getStepContext().getJobParameters().get("run.date"));
        int emitted = service.emitDue(runDate);
        chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getExecutionContext().putInt("emitted", emitted);
        return RepeatStatus.FINISHED;
    }
}
