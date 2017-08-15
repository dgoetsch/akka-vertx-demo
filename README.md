## Akka-Vert.x Demo

This project demonstrates fundamental differences between akka and vert.x.

1. First, start cassandra with `docker-compose up`
1. Then, put the ip of the cassandra container in `src/main/resources/application.local.properties` for the api you are running
1. Run the application
    * To run the vert.x application: `gradle vertx-api:run`
    * To run the akka application: `gradle akka-base:run`
1. Run the command line client: `gradle cmd-interface:run`

###Limitations
The akka application does not yet have web sockets, because of this the command line client is not yet fully operational.
