package org.swissbib.mf.mongo.preprocessing;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultPreprocessing {

    //private final static Pattern pRecord;
    //private final static Pattern pRecordRoot;
    private final static String replacement;

    private final static HashMap<String, MatcherCriteria> matchersList;

    static {

        matchersList = new HashMap<>();
        matchersList.put("DEFAULT",new MatcherCriteria(
                Pattern.compile(".*?(<marc:record.*?</marc:record>).*?</metadata></record>",
                        Pattern.MULTILINE |
                                Pattern.CASE_INSENSITIVE |
                                Pattern.DOTALL),
                Pattern.compile("<marc:record.*?>",
                        Pattern.MULTILINE |
                                Pattern.CASE_INSENSITIVE |
                                Pattern.DOTALL)
        ) );


        matchersList.put("SNL",new MatcherCriteria(
                Pattern.compile(".*?(<marc:record>.*?</marc:record>).*?",
                        Pattern.MULTILINE |
                                Pattern.CASE_INSENSITIVE |
                                Pattern.DOTALL),
                Pattern.compile("<marc:record.*?>",
                        Pattern.MULTILINE |
                                Pattern.CASE_INSENSITIVE |
                                Pattern.DOTALL)
        ) );



        replacement = "<marc:record type=\"Bibliographic\" xmlns:marc=\"http://www.loc.gov/MARC21/slim\">";

    }


    public static String cleanRecord(String dbRecord, String matcher)
    {

        Matcher m  = matchersList.get(matcher).getpRecord().matcher(dbRecord);
        String recordBody = "";
        if (m.matches())
            recordBody = m.group(1);
            m =  matchersList.get(matcher).getpRecordRoot().matcher(recordBody);
            recordBody = m.replaceFirst(replacement);


        //System.out.println(recordBody);
        return recordBody;

    }

    private static class MatcherCriteria {


        Pattern pRecord;
        Pattern pRecordRoot;

        MatcherCriteria(Pattern pRecord, Pattern pRecordRoot) {
            this.pRecord = pRecord;
            this.pRecordRoot = pRecordRoot;
        }

        public Pattern getpRecord() {
            return pRecord;
        }

        public Pattern getpRecordRoot() {
            return pRecordRoot;
        }
    }

}
