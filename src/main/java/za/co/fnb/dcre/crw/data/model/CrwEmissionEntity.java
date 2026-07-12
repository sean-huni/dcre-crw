package za.co.fnb.dcre.crw.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.time.LocalDate;
import java.util.UUID;

@Table("crw_emission")
public class CrwEmissionEntity extends BaseEntity {

    private UUID arrivalId;
    private LocalDate runDate;
    private String fileName;
    private String state;

    public static CrwEmissionEntity planned(UUID arrivalId, LocalDate runDate, String fileName) {
        CrwEmissionEntity e = new CrwEmissionEntity();
        e.assignIdIfMissing();
        e.arrivalId = arrivalId;
        e.runDate = runDate;
        e.fileName = fileName;
        e.state = "PLANNED";
        return e;
    }

    public UUID getArrivalId() { return arrivalId; }
    public LocalDate getRunDate() { return runDate; }
    public String getFileName() { return fileName; }
    public String getState() { return state; }
}
