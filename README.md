[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven]

# OpenTracing Hazelcast Instrumentation
OpenTracing instrumentation for Hazelcast.

## Requirements
- Java 8+

## Installation

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-hazelcast</artifactId>
    <version>VERSION</version>
</dependency>
```

## Usage

```java
// Instantiate tracer
Tracer tracer = ...

// Register tracer with GlobalTracer:
GlobalTracer.register(tracer);

// Decorate HazelcastInstance with Tracing HazelcastInstance:
HazelcastInstance hazelcast = new TracingHazelcastInstanve(Hazelcast.newHazelcastInstance(config), 
                                                           false);

// Get Map: 
ConcurrentMap<String, String> map = hazelcast.getMap("distributed-map");
map.put("key", "value");


```

## License

[Apache 2.0 License](./LICENSE).

[ci-img]: https://travis-ci.org/opentracing-contrib/java-hazelcast.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-hazelcast
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-hazelcast/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-hazelcast?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-hazelcast.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-hazelcast

