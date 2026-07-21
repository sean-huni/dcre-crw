package za.co.fnb.dcre.crw.bdd;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import za.co.fnb.dcre.crw.service.EmissionService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Step definitions for the @crw R-37 executor scenarios (seeding mirrors CrwJobTest). */
public class CrwEmissionSteps {

    @Autowired
    Job crwJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    private UUID arrival;
    private String msgId;
    private int dueCount;
    private Path painFile;
    private JobExecution lastExecution;
    private List<String> savedXml;
    private ListAppender<ILoggingEvent> warnAppender;
    private Logger emissionLogger;
    // Job identity is (run.date, window); a per-scenario salt keeps logically
    // identical windows of different scenarios from colliding as one instance.
    private String windowSalt;
    private int attempt;

    @Before
    public void setUp() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35),"
                + " flow VARCHAR(8) NOT NULL DEFAULT 'COL',"
                + " created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_entry (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, e2e VARCHAR(35), amount DECIMAL(18,2), UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS cde_schedule (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, process_date DATE, UNIQUE (arrival_id, sequence))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS ais_verdict (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, action VARCHAR(16), UNIQUE (arrival_id, sequence))");
        emissionLogger = (Logger) LoggerFactory.getLogger(EmissionService.class);
        warnAppender = new ListAppender<>();
        warnAppender.start();
        emissionLogger.addAppender(warnAppender);
    }

    @After
    public void tearDown() {
        emissionLogger.detachAppender(warnAppender);
    }

    @Given("a DC arrival with {int} transactions due on {} and {int} futured to {}")
    public void arrivalWithDueAndFuturedTransactions(int due, String dueDate, int futured, String futureDate) {
        arrival = UUID.randomUUID();
        windowSalt = arrival.toString().substring(0, 8);
        msgId = "DCRERFCRW" + arrival.toString().substring(0, 6);
        dueCount = due;
        // SCRUM-42: per-client fint-req/out leaf (client FNBRF01 -> base fnbrf01).
        painFile = Path.of("build/test-exchange/fnbrf01/fint-req/out", "FNBRF01_" + msgId + "_PAIN008.xml");
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty) VALUES (?,?,?)",
                arrival, msgId, "FNBRF01");
        for (int i = 1; i <= due + futured; i++) {
            jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, e2e, amount) VALUES (?,?,?,?)"
                    + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, i, "E2E" + msgId + i, i * 10.0);
            jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')"
                    + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, i);
            jdbc.update("INSERT INTO cde_schedule (arrival_id, sequence, process_date) VALUES (?,?,?::date)"
                    + " ON CONFLICT (arrival_id, sequence) DO NOTHING", arrival, i,
                    i <= due ? dueDate : futureDate);
        }
    }

    @Given("the CRW job has run for {} in window {string}")
    public void crwJobHasRun(String runDate, String window) throws Exception {
        startJob(runDate, window);
        assertEquals(BatchStatus.COMPLETED, lastExecution.getStatus());
        savedXml = Files.exists(painFile) ? Files.readAllLines(painFile) : null;
    }

    @When("the CRW job runs for {} in window {string}")
    public void crwJobRuns(String runDate, String window) throws Exception {
        startJob(runDate, window);
    }

    @When("the CRW job runs again for {} in window {string}")
    public void crwJobRunsAgain(String runDate, String window) throws Exception {
        // Spring Batch refuses to restart a COMPLETED instance with identical
        // identifying parameters, so the operational rerun of the same business
        // window carries an attempt counter (same pattern as CdeJobTest).
        attempt++;
        startJob(runDate, window);
    }

    @When("the schedule drifts so one futured transaction becomes due on {}")
    public void scheduleDrifts(String newDueDate) {
        int updated = jdbc.update("UPDATE cde_schedule SET process_date=?::date"
                        + " WHERE arrival_id=? AND sequence=(SELECT min(sequence) FROM cde_schedule"
                        + " WHERE arrival_id=? AND process_date>?::date)",
                newDueDate, arrival, arrival, newDueDate);
        assertEquals(1, updated, "exactly one futured row drifts into the due set");
    }

    @When("the emission is rolled back to state MATERIALIZED")
    public void emissionRolledBack() {
        assertEquals(1, jdbc.update(
                "UPDATE crw_emission SET state='MATERIALIZED' WHERE arrival_id=?", arrival));
    }

    @When("the emitted pain.008 file is deleted")
    public void painFileDeleted() throws Exception {
        Files.delete(painFile);
    }

    @Then("the CRW job completes")
    public void jobCompletes() {
        assertEquals(BatchStatus.COMPLETED, lastExecution.getStatus());
    }

    @Then("the pain.008 for the arrival contains {int} transactions")
    public void painFileContainsTransactions(int expected) throws Exception {
        List<String> xml = Files.readAllLines(painFile);
        assertEquals(1, xml.stream().filter(l -> l.contains("<NbOfTxs>" + expected + "</NbOfTxs>")).count(),
                "NbOfTxs must equal the due-transaction count");
        assertEquals(expected, xml.stream().filter(l -> l.contains("<DrctDbtTxInf>")).count(),
                "one DrctDbtTxInf per due transaction");
    }

    @Then("the pain.008 carries the TT2 local instrument")
    public void painFileCarriesTt2() throws Exception {
        assertTrue(Files.readAllLines(painFile).stream()
                .anyMatch(l -> l.contains("<LclInstrm><Cd>TT2</Cd></LclInstrm>")), "R-02 TT2 LclInstrm");
    }

    @Then("no pain.008 file exists for the arrival")
    public void noPainFileExists() {
        assertTrue(Files.notExists(painFile), "VISIBLE emission must not be re-emitted");
    }

    @Then("the pain.008 file is unchanged")
    public void painFileUnchanged() throws Exception {
        assertNotNull(savedXml, "a prior emission snapshot of the file is required");
        assertEquals(savedXml, Files.readAllLines(painFile), "rerun must not alter the emitted file");
    }

    @Then("{int} exclusion warnings are logged for stage CRW with reason {}")
    public void exclusionWarningsLogged(int expected, String reason) {
        List<String> warns = arrivalWarns().stream()
                .filter(m -> m.contains("reason=" + reason))
                .toList();
        assertEquals(expected, warns.size(), "one WARN per warehoused (futured) transaction");
        for (String warn : warns) {
            assertTrue(warn.contains("seq="), warn);
            assertTrue(warn.contains("e2e=E2E" + msgId), warn);
        }
    }

    @Then("a file-level exclusion warning is logged for stage CRW with reason ALREADY_VISIBLE")
    public void alreadyVisibleWarningLogged() {
        assertTrue(arrivalWarns().stream()
                        .anyMatch(m -> m.equals("excluded stage=CRW arrival=" + arrival
                                + " seq=-1 e2e=- reason=ALREADY_VISIBLE batch=1")),
                "single file-level ALREADY_VISIBLE WARN (seq=-1, e2e=-, SCRUM-55 batch grain)");
    }

    private List<String> arrivalWarns() {
        return warnAppender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains("excluded stage=CRW") && m.contains("arrival=" + arrival))
                .toList();
    }

    private void startJob(String runDate, String window) throws Exception {
        JobParametersBuilder params = new JobParametersBuilder()
                .addString("run.date", runDate, true)
                .addString("window", window + "-" + windowSalt, true);
        if (attempt > 0) {
            params.addString("attempt", String.valueOf(attempt), true);
        }
        lastExecution = jobOperator.start(crwJob, params.toJobParameters());
    }
}
