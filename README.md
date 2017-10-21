## Akka-Vert.x Demo

This project demonstrates fundamental differences between akka and vert.x.

To run the demo:

1. First, start cassandra with `docker-compose up`
1. Run the application (run vert.x first because akka doesnt' automatically initialize the schema on startup)
    * To run the vert.x application: `gradle vertx-api:run`
    * To run the akka application: `gradle akka-base:run`
1. Run the command line client: `gradle cmd-interface:run`


## Modules

* akka-api - the akka demo application
* vertx-api - the vert.x demo application
* cmd-interface - the command line client for demonstrating both apis
* cassandra - some Cassandra utility classes
* event-log - a Cassandra based implementation of an event sourcing data model
* order - an implementation of an order API on top of event-log
