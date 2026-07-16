package za.co.fnb.dcre.crw.batch;

import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import za.co.fnb.dcre.crw.service.EmissionService;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * SCRUM-55 review fix: lanes are keyed by BOUNDED index (lane-N), never by
 * the unbounded comma-joined client tokens. Worker step executions persist
 * as emitWorkerStep:&lt;key&gt; into CRW_BATCH_STEP_EXECUTION.STEP_NAME
 * (VARCHAR(100)); with initg_pty at VARCHAR(35), 3+ long clients in one lane
 * overflowed the column at runtime. The client list travels in the lane's
 * ExecutionContext instead (SHORT_CONTEXT is VARCHAR(2500)).
 */
class ClientLanePartitionerTest {

    private static final LocalDate RUN_DATE = LocalDate.of(2026, 7, 16);
    private static final String WORKER_STEP_PREFIX = "emitWorkerStep:";

    private ClientLanePartitioner partitionerFor(final List<String> clients) {
        EmissionService service = mock(EmissionService.class);
        when(service.dueClients(RUN_DATE)).thenReturn(clients);
        return new ClientLanePartitioner(service, RUN_DATE);
    }

    @Test
    void lanesAreKeyedByBoundedIndexNeverByClientTokens() {
        // 15 due clients at initg_pty's VARCHAR(35) ceiling: token-keyed step
        // names would exceed STEP_NAME VARCHAR(100) from 3 clients per lane.
        List<String> clients = IntStream.range(0, 15)
                .mapToObj(i -> ("FNBLONG%02d".formatted(i) + "X".repeat(35)).substring(0, 35))
                .toList();

        Map<String, ExecutionContext> partitions = partitionerFor(clients).partition(5);

        assertThat(partitions.keySet()).containsExactly("lane-0", "lane-1", "lane-2", "lane-3", "lane-4");
        for (final Map.Entry<String, ExecutionContext> lane : partitions.entrySet()) {
            assertThat(WORKER_STEP_PREFIX.length() + lane.getKey().length())
                    .as("persisted worker step name must fit STEP_NAME VARCHAR(100)")
                    .isLessThanOrEqualTo(100);
            assertThat(lane.getValue().getString("clients").split(",")).hasSize(3);
        }
    }

    @Test
    void laneContextCarriesTheRoundRobinClientList() {
        Map<String, ExecutionContext> partitions =
                partitionerFor(List.of("A", "B", "C", "D", "E")).partition(2);

        assertThat(partitions.get("lane-0").getString("clients")).isEqualTo("A,C,E");
        assertThat(partitions.get("lane-1").getString("clients")).isEqualTo("B,D");
    }

    @Test
    void singleLaneCollapsesAllClientsInInsertionOrder() {
        Map<String, ExecutionContext> partitions =
                partitionerFor(List.of("FNBRF01", "FNBCC01")).partition(1);

        assertThat(partitions.keySet()).containsExactly("lane-0");
        assertThat(partitions.get("lane-0").getString("clients")).isEqualTo("FNBRF01,FNBCC01");
    }

    @Test
    void emptyDueSetProducesNoLanes() {
        assertThat(partitionerFor(List.of()).partition(5)).isEmpty();
    }
}
