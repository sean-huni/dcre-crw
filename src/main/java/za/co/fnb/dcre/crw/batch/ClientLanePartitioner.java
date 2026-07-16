package za.co.fnb.dcre.crw.batch;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import za.co.fnb.dcre.crw.service.EmissionService;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SCRUM-55 Feature 2: one partition per client lane, keyed by BOUNDED index
 * (lane-N). Worker executions persist as emitWorkerStep:&lt;key&gt; into
 * CRW_BATCH_STEP_EXECUTION.STEP_NAME (VARCHAR(100)), so the unbounded client
 * tokens must never ride in the key (review fix: 3+ VARCHAR(35) clients in
 * one lane overflowed the column at runtime); the comma-joined client list
 * travels in the lane's ExecutionContext (SHORT_CONTEXT VARCHAR(2500)).
 * gridSize arrives as PartitionSizer.partitions(maxPartitions) = min(pod
 * CPUs, DCRE_CRW_MAX_PARTITIONS) and is a REAL concurrency cap: the partition
 * handler runs every returned partition concurrently on virtual threads, so
 * when there are more due clients than lanes the clients bucket round-robin
 * and a lane works its clients serially (normal case stays one client per
 * lane). FIFO within a client is preserved by the client-scoped due query.
 */
public class ClientLanePartitioner implements Partitioner {

    private final EmissionService service;
    private final LocalDate runDate;

    public ClientLanePartitioner(final EmissionService service, final LocalDate runDate) {
        this.service = service;
        this.runDate = runDate;
    }

    @Override
    public Map<String, ExecutionContext> partition(final int gridSize) {
        final List<String> clients = service.dueClients(runDate);
        final Map<String, ExecutionContext> partitions = new LinkedHashMap<>();
        if (clients.isEmpty()) {
            return partitions;
        }
        final int lanes = Math.max(1, Math.min(gridSize, clients.size()));
        final List<List<String>> buckets = new ArrayList<>(lanes);
        for (int i = 0; i < lanes; i++) {
            buckets.add(new ArrayList<>());
        }
        for (int i = 0; i < clients.size(); i++) {
            buckets.get(i % lanes).add(clients.get(i));
        }
        for (int lane = 0; lane < buckets.size(); lane++) {
            final ExecutionContext context = new ExecutionContext();
            context.putString("clients", String.join(",", buckets.get(lane)));
            partitions.put("lane-" + lane, context);
        }
        return partitions;
    }
}
