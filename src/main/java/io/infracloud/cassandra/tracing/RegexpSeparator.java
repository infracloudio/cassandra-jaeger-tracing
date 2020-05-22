/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.infracloud.cassandra.tracing;

import io.opentracing.Span;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class serves to identify common Cassandra trace messages,
 * splitting them with regexes to be set as tags
 */
public class RegexpSeparator {

    final static private SingleRegexp[] regexps = {
            new SingleRegexp("Key cache hit",
                    "Key cache hit for sstable (?<sstableid>\\d+)",
                    new String[]{"sstableid"}),
            new SingleRegexp("Parsing query",
                    "Parsing (?<query>.*)",
                    new String[]{"query"}),
            new SingleRegexp("Reading data",
                    "reading data from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Read live rows and tombstone cells",
                    "Read (?<liverows>\\d+) live rows and (?<tombstonecells>\\d+) tombstone cells",
                    new String[]{"liverows", "tombstonecells"}),
            new SingleRegexp("Merged data from memtables and sstables",
                    "Merged data from memtables and (?<sstables>\\d+) sstables",
                    new String[]{"sstables"}),
            new SingleRegexp("Skipped non-slice-intersecting sstables",
                    "Skipped (?<sstables>\\d+/\\d+) non-slice-intersecting sstables, included (?<tombstones>\\d+) due to tombstones",
                    new String[]{"sstables", "tombstones"}),
            new SingleRegexp("Received READ message",
                    "READ message received from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Enqueuing response",
                    "Enqueuing response to (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Sending response",
                    "Sending REQUEST_RESPONSE message to (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("REQUEST_RESPONSE message received",
                    "REQUEST_RESPONSE message received from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Sending READ message",
                    "Sending READ message to (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Scanned rows and matched ",
                    "Scanned (?<rows>\\d+) rows and matched (?<matched>\\d+)",
                    new String[]{"rows", "matched"}),
            new SingleRegexp("Partition index found",
                    "Partition index with (?<entries>\\d+) entries found for sstable (?<sstableid>\\d+)",
                    new String[]{"entries", "sstableid"}),
            new SingleRegexp("Speculating read retry",
                    "speculating read retry on (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Processing response",
                    "Processing response from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Bloom filter allows skipping sstable",
                    "Bloom filter allows skipping sstable (?<sstableid>\\d+)",
                    new String[]{"sstableid"}),
            new SingleRegexp("Caching rows",
                    "Caching (?<rows>\\d+) rows",
                    new String[]{"rows"}),
            new SingleRegexp("Executing single-partition query",
                    "Executing single-partition query on (?<table>.*)",
                    new String[]{"table"}),
            new SingleRegexp("Sending MUTATION message",
                    "Sending MUTATION message to (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Adding to memtable",
                    "Adding to (?<table>.*) memtable",
                    new String[]{"table"}),
            new SingleRegexp("Received MUTATION message",
                    "MUTATION message received from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Reading digest",
                    "reading digest from (?<othernode>.*)",
                    new String[]{"othernode"}),
            new SingleRegexp("Read-repair",
                    "Read-repair (?<consistency>.*)",
                    new String[]{"consistency"})
    };

    static public AnalysisResult match(String trace) {
        for (final SingleRegexp srp : regexps) {
            final Matcher match = srp.match(trace);
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

        /**
         * Apply extracted tags to the span, or a no-op in case of a NoMatch
         */
        public void applyTags(Span span) {
        }
    }

    private static class NoMatch extends AnalysisResult {
        final private String trace;

        private NoMatch(String trace) {
            this.trace = trace;
        }

        @Override
        public String getTraceName() {
            return trace;
        }
    }

    private static class RegexpResult extends AnalysisResult {
        final private SingleRegexp srp;
        final private Matcher match;

        private RegexpResult(SingleRegexp srp, Matcher match) {
            this.srp = srp;
            this.match = match;
        }

        @Override
        public String getTraceName() {
            return srp.label;
        }

        @Override
        public void applyTags(Span span) {
            for (final String groupName : srp.namedGroups) {
                span.setTag(groupName, match.group(groupName));
            }
        }
    }

    private static class SingleRegexp {
        final private String label;
        final private Pattern pattern;
        // we provide named groups explicitly since there's no Java API to get
        // names of the groups from the matcher
        final private String[] namedGroups;

        private SingleRegexp(String label, String regexp, String[] namedGroups) {
            this.pattern = Pattern.compile(regexp);
            this.label = label;
            this.namedGroups = namedGroups;
        }

        private Matcher match(String trace) {
            return pattern.matcher(trace);
        }
    }
}
