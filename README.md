# A Jaeger tracing plugin for Cassandra

![stability-wip](https://img.shields.io/badge/stability-archived-lightgrey.svg)

**Update**: This repository is no longer actively maintained. One of the contributors, [piotrmaslanka](https://github.com/piotrmaslanka), has taken over the project's maintenance.  
Huge thanks to Piotr for picking this up and keeping the project going!  
The updated repository can be found at <https://github.com/smok-serwis/cassandra-jaeger-tracing>.

---

This plugin is based on [A Zipkin tracing plugin for
Cassandra](https://github.com/thelastpickle/cassandra-zipkin-tracing)
which is licensed under [Apache License
2.0](https://github.com/thelastpickle/cassandra-zipkin-tracing/blob/master/LICENSE.txt).

Cassandra provides [pluggable
tracing](https://web.archive.org/web/20160402125018/http://www.planetcassandra.org/blog/cassandra-3-4-release-overview/)
starting from version 3.4. By adding a jar file to the Cassandra
classpath and one JVM option, Cassandra's tracing can be replaced with
Jaeger. It can even identify incoming Jaeger traces and add
Cassandra's own internal tracing on to it.

## How to use
- Make sure you have [Maven](https://maven.apache.org/) and
  [JDK](https://openjdk.java.net/) installed on your machine
- Run following commands to build and place the jar
  ```sh
  # Cloning the repository
  git clone https://github.com/infracloudio/cassandra-jaeger-tracing.git
  cd cassandra-jaeger-tracing

  # Create a jar file
  mvn package
  cp target/cassandra-jaeger-tracing-*-jar-with-dependencies.jar $CASSANDRA_HOME/lib/
  ```
  Here, `$CASSANDRA_HOME` is the directory where Cassandra is installed
- Start Cassandra with,
  ```sh
  JVM_OPTS\
  ="-Dcassandra.custom_tracing_class=io.infracloud.cassandra.tracing.JaegerTracing" \
  cassandra
  ```
  or edit the `jvm.options`

By default
[`jaeger-client-java`](https://github.com/jaegertracing/jaeger-client-java)
sends the spans to `localhost:6831` via UDP. This can be configured by
setting environment variables, `JAEGER_AGENT_HOST` and
`JAEGER_AGENT_PORT`. Refer [Configuration via
Environment](https://github.com/jaegertracing/jaeger-client-java/tree/master/jaeger-core#configuration-via-environment)
for more information.

![cassandra-jaeger-tracing-select-query](https://user-images.githubusercontent.com/5154532/55792869-2ebf3300-5adf-11e9-9326-ad65f0e564ec.png
"SELECT * FROM t;")

## Background
See
[CASSANDRA-10392](https://issues.apache.org/jira/browse/CASSANDRA-10392)
for the patch to extend Cassandra's tracing that this project plugs
into.

## Troubleshooting

When this tracing is used instead of Cassandra's default tracing, any
cqlsh statements run after enabling tracing with `TRACING ON;` are
going to time out eventually giving

```
Unable to fetch query trace: Trace information was not available within …
```

This is because cqlsh is polling for tracing information in
`system_traces` which isn't any longer being created. For the meantime
an easy fix around this behaviour in cqlsh is to reduce
`Session.max_trace_wait` down to 1 second.

### Continuing parent traces

In order to continue a parent trace send the trace injected
into custom_payload with the _trace_id_key_. Default is `uber-trace-id`, but it can be changed through an environment variable.
Inject it using HTTP_HEADERS TextMap codec with url encoding value of true.

Refer to your Cassandra driver documentation in order
to figure out how to send custom_payload.

If you need a custom trace key, specify it in environment
variable `JAEGER_TRACE_KEY`. Note that the default 
is `uber-trace-id`.
