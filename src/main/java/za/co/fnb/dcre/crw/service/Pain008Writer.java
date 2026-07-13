package za.co.fnb.dcre.crw.service;

import org.springframework.stereotype.Component;
import za.co.fnb.dcre.crw.data.model.CrwEmissionMemberEntity;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * SYNTHETIC-CONTRACT (R-35, A-9): pain.008-shaped XML skeleton. Real bindings
 * become JAXB from the Fintegrate XSD profile when recovered (R-18); outbound
 * EndToEndId is the canonical value byte-preserved (R-15).
 */
@Component
public class Pain008Writer {

    public List<String> build(String msgId, List<CrwEmissionMemberEntity> members, BigDecimal controlSum) {
        List<String> xml = new ArrayList<>();
        xml.add("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.add("<Document><!-- SYNTHETIC-CONTRACT pain.008 skeleton (A-9) -->");
        xml.add("  <CstmrDrctDbtInitn><GrpHdr>");
        xml.add("    <MsgId>%s</MsgId>".formatted(msgId));
        xml.add("    <NbOfTxs>%d</NbOfTxs>".formatted(members.size()));
        xml.add("    <CtrlSum>%s</CtrlSum>".formatted(controlSum));
        xml.add("  </GrpHdr><PmtInf>");
        xml.add("    <PmtTpInf><LclInstrm><Cd>TT2</Cd></LclInstrm></PmtTpInf>");
        for (CrwEmissionMemberEntity member : members) {
            xml.add("    <DrctDbtTxInf><PmtId><EndToEndId>%s</EndToEndId></PmtId><InstdAmt Ccy=\"ZAR\">%s</InstdAmt></DrctDbtTxInf>".formatted(member.getE2e().strip(), member.getAmount()));
        }
        xml.add("  </PmtInf></CstmrDrctDbtInitn>");
        xml.add("</Document>");
        return xml;
    }
}
