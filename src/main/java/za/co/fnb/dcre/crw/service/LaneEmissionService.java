package za.co.fnb.dcre.crw.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Business tier for one partitioned client lane (SCRUM-55 Feature 2). Lanes
 * run in parallel; WITHIN a lane the clients are serial and each client's
 * parents follow insertion-order FIFO eligibility (client-scoped due query).
 * A failed client is logged and skipped so sibling clients in the same lane
 * still emit; the lane then fails to keep the window outcome honest (the
 * next window resumes exactly the unplanned arrivals and unpublished
 * batches, SCRUM-55 durable-effect ordering).
 */
@Service
public class LaneEmissionService {

    private static final Logger log = LoggerFactory.getLogger(LaneEmissionService.class);

    private final EmissionService emission;

    public LaneEmissionService(final EmissionService emission) {
        this.emission = emission;
    }

    /** @return number of pain.008 FILES emitted by this lane (batch grain). */
    public int emitLane(final LocalDate runDate, final List<String> clients) {
        int emitted = 0;
        final List<String> failed = new ArrayList<>();
        for (final String client : clients) {
            try {
                emitted += emission.emitDue(runDate, client);
            } catch (final RuntimeException e) {
                failed.add(client);
                log.error("lane emission failed stage=CRW client={} runDate={}", client, runDate, e);
            }
        }
        if (!failed.isEmpty()) {
            throw new IllegalStateException("%d of %d lane clients failed emission for run date %s: %s"
                    .formatted(failed.size(), clients.size(), runDate, failed));
        }
        return emitted;
    }
}
