package za.co.fnb.dcre.crw.data.repo;

import java.time.LocalDate;
import java.util.UUID;

/**
 * The member claim: the FOURTH site reading the shared eligible-row predicate, after the due
 * queries, the plan totals and the batch boundaries.
 *
 * <p>It was found by the compiler when the shared fragment moved, not by inspection, which is
 * the point: three sites had been enumerated by hand and this was not among them. Anything
 * applied to the query sites alone would have moved a failure from due detection into planning
 * rather than dealing with it.
 */
public interface CrwMemberClaims {

    /**
     * Set-based ordinal member claim (SCRUM-55): the [loSeq, hiSeq] slice of the eligible rows is
     * claimed in ONE statement inside the arrival transaction; hiSeq -1 means the open-ended
     * tail. No Java list of 300k rows ever (SCRUM-42 memory lesson).
     */
    void claimMembers(UUID emissionId, LocalDate runDate, UUID arrivalId, int loSeq, int hiSeq);
}
