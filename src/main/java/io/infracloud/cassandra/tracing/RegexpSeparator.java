package io.infracloud.cassandra.tracing;

import io.opentracing.Span;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class serves to identify common Cassandra trace messages,
 * splitting them with regexes to be set as tags
 */
public class RegexpSeparator {

    final static private SingleRegexpSeparator[] regexes = {
            new SingleRegexpSeparator("Key cache hit",
                    "Key cache hit for sstable (?<sstableid>\\d+)",
                    new String[]{"sstableid"}),
            new SingleRegexpSeparator("Parsing query",
                    "Parsing (?<query>.*)",
                    new String[]{"query"}),
            new SingleRegexpSeparator("Reading data",
                    "reading data from /(?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexpSeparator("Read live rows and tombstone cells",
                    "Read (?<liverows>\\d+) live rows and (?<tombstonecells>\\d+) tombstone cells",
                    new String[]{"liverows", "tombstonecells"}),
            new SingleRegexpSeparator("Merged data from memtables and sstables",
                    "Merged data from memtables and (?<sstables>\\d+) sstables",
                    new String[]{"sstables"}),
            new SingleRegexpSeparator("Skipped non-slice-intersecting sstables",
                    "Skipped (?<howmany>\\d+/\\d+) non-slice-intersecting sstables, included (?<tombstones>\\d+) due to tombstones",
                    new String[]{"howmany", "tombstones"}),
            new SingleRegexpSeparator("Received READ message",
                    "READ message received from /(?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexpSeparator("Enqueuing response",
                    "Enqueuing response to /(?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexpSeparator("Sending response",
                    "Sending REQUEST_RESPONSE message to /(?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexpSeparator("REQUEST_RESPONSE message received",
                    "REQUEST_RESPONSE message received from /(?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexpSeparator("Sending READ message",
                    "Sending READ message to /(?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexpSeparator("Scanned rows and matched ",
                    "Scanned (?<rows>\\d+) rows and matched (?<matched>\\d+)",
                    new String[]{"rows", "matched"}),
            new SingleRegexpSeparator("Partition index found",
                    "Partition index with (?<entries>\\d+) found for sstable (?<sstableid>\\d+)",
                    new String[]{"entries", "sstableid"}),
            new SingleRegexpSeparator("Speculating read retry",
                    "speculating read retry on /(?<othernode>.*)",
                    new String[]{"othernode"})
    };

    static protected AnalysisResult match(String trace) {
        for (SingleRegexpSeparator srp : regexes) {
            Matcher match = srp.match(trace);
            if (match.matches()) {
                return new RegexpResult(srp, match);
            }
        }
        return new NoMatch(trace);
    }

    abstract static public class AnalysisResult {
        /**
         * Return the name of the trace to use in Jaeger
         */
        abstract public String getTraceName();

        public void applyTags(Span span) {
        }
    }

    public static class NoMatch extends AnalysisResult {
        final private String trace;

        protected NoMatch(String trace) {
            this.trace = trace;
        }

        public String getTraceName() {
            return trace;
        }
    }

    public static class RegexpResult extends AnalysisResult {
        final private SingleRegexpSeparator srp;
        final private Matcher match;

        public RegexpResult(SingleRegexpSeparator srp, Matcher match) {
            this.srp = srp;
            this.match = match;
        }

        @Override
        public String getTraceName() {
            return srp.label;
        }

        @Override
        public void applyTags(Span span) {
            for (String groupName : srp.namedGroups) {
                span.setTag(groupName, match.group(groupName));
            }
        }
    }

    private static class SingleRegexpSeparator {
        final private String label;
        final private Pattern pattern;
        // we provide named groups explicitly since there's no Java API to get
        // names of the groups from the matcher
        final private String[] namedGroups;

        protected SingleRegexpSeparator(String label, String regexp, String[] namedGroups) {
            pattern = Pattern.compile(regexp);
            this.label = label;
            this.namedGroups = namedGroups;
        }

        protected Matcher match(String trace) {
            return pattern.matcher(trace);
        }
    }
}
