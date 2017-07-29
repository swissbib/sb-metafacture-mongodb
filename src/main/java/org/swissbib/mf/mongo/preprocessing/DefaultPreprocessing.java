package org.swissbib.mf.mongo.preprocessing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultPreprocessing {

    private final static Pattern pRecord;
    private final static Pattern pRecordRoot;
    private final static String replacement;
    static {
        pRecord = Pattern.compile(".*?(<marc:record.*?</marc:record>).*?</metadata></record>",
                Pattern.MULTILINE |
                Pattern.CASE_INSENSITIVE |
                Pattern.DOTALL);

        pRecordRoot = Pattern.compile("<marc:record.*?>",
                Pattern.MULTILINE |
                        Pattern.CASE_INSENSITIVE |
                        Pattern.DOTALL);
        replacement = "<marc:record type=\"Bibliographic\" xmlns:marc=\"http://www.loc.gov/MARC21/slim\">";

    }


    public static String cleanRecord(String dbRecord)
    {

        Matcher m  = pRecord.matcher(dbRecord);
        String recordBody = "";
        if (m.matches())
            recordBody = m.group(1);
            m = pRecordRoot.matcher(recordBody);
            recordBody = m.replaceFirst(replacement);


        //System.out.println(recordBody);
        return recordBody;

    }



}
