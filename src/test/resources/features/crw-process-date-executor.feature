@crw
Feature: CRW emits only transactions due on the run date (R-37)

  CRW is the Process-Date Executor: a window run emits a pain.008 collection
  order containing ONLY the transactions whose scheduled process date equals
  the run date. Futured work stays warehoused (visible via WARN), membership
  is snapshotted immutably before any file is built (R-24), and a collection
  order handed to Fintegrate is never emitted twice.

  Scenario: Only transactions due on the run date are emitted into the pain.008
    Given a DC arrival with 3 transactions due on 2026-07-14 and 3 futured to 2026-07-20
    When the CRW job runs for 2026-07-14 in window "w1"
    Then the CRW job completes
    And the pain.008 for the arrival contains 3 transactions
    And the pain.008 carries the TT2 local instrument

  Scenario: Futured transactions stay warehoused and each is logged as a WARN
    Given a DC arrival with 2 transactions due on 2026-07-14 and 4 futured to 2026-07-20
    When the CRW job runs for 2026-07-14 in window "w1"
    Then the CRW job completes
    And the pain.008 for the arrival contains 2 transactions
    And 4 exclusion warnings are logged for stage CRW with reason FUTURED_2026-07-20

  Scenario: A restart rebuilds the identical member set from the immutable snapshot
    Given a DC arrival with 3 transactions due on 2026-07-14 and 3 futured to 2026-07-20
    And the CRW job has run for 2026-07-14 in window "w1"
    When the schedule drifts so one futured transaction becomes due on 2026-07-14
    And the emission is rolled back to state MATERIALIZED
    And the emitted pain.008 file is deleted
    And the CRW job runs for 2026-07-14 in window "w2"
    Then the CRW job completes
    And the pain.008 for the arrival contains 3 transactions

  Scenario: A later window never re-emits a VISIBLE collection order
    Given a DC arrival with 3 transactions due on 2026-07-14 and 3 futured to 2026-07-20
    And the CRW job has run for 2026-07-14 in window "w1"
    And the emitted pain.008 file is deleted
    When the CRW job runs for 2026-07-14 in window "w2"
    Then the CRW job completes
    And no pain.008 file exists for the arrival
    And a file-level exclusion warning is logged for stage CRW with reason ALREADY_VISIBLE

  Scenario: Rerunning the exact same window is idempotent
    Given a DC arrival with 3 transactions due on 2026-07-14 and 3 futured to 2026-07-20
    And the CRW job has run for 2026-07-14 in window "w1"
    When the CRW job runs again for 2026-07-14 in window "w1"
    Then the CRW job completes
    And the pain.008 for the arrival contains 3 transactions
    And the pain.008 file is unchanged
