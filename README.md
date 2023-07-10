STOMP protocol client and server for [Swaptacular]
==================================================

This project implements [STOMP protocol] message transport for the
[Swaptacular Messaging Protocol]. The ultimate deliverable is a [docker
image], generated from the project's [Dockerfile](../master/Dockerfile).

**Note:** This implementation supports only [JSON serialization for the
Swaptacular Messaging Protocol].


Dependencies
------------

Containers started from the generated docker image must have access to
the following servers:

1. [PostgreSQL] server instance, which stores accounts' data.

2. [RabbitMQ] server instance, which acts as broker for [Swaptacular
   Messaging Protocol] (SMP) messages.

   A [RabbitMQ queue] must be configured on the broker instance, so
   that all incoming SMP messages for the accounts stored on the
   PostgreSQL server instance, are routed to this queue.

   Also, the following [RabbitMQ exchanges] must be configured on the
   broker instance:

   - **`to_creditors`**: For messages that must be send to the
     creditors agents. The routing key will represent the creditor ID
     as hexadecimal. For example, for creditor ID equal to 2, the
     routing key will be "00.00.00.00.00.00.00.02".

   - **`to_debtors`**: For messages that must be send to the debtors
     agents. The routing key will represent the debtor ID as
     hexadecimal. For example, for debtor ID equal to -2, the routing
     key will be "ff.ff.ff.ff.ff.ff.ff.fe".

   - **`to_coordinators`**: For messages that must be send to the
     transfer coordinators. Different types of transfer coordinators
     are responsible for performing different types of transfers. The
     most important types are: "direct" (the message must be sent to
     the creditors agent), and "issuing" (the message must be sent to
     the debtors agent). All the messages sent to this exchange, will
     have a correctly set "coordinator_type" header. The routing key
     will represent the coordinator ID as hexadecimal. Note that for
     "direct" transfers, the coordinator ID is guaranteed to be the
     same as the creditor ID; and for "issuing" transfers, the
     coordinator ID is guaranteed to be the same as the debtor ID.

   - **`accounts_in`**: For messages that must be send to this
     accounting authority itself (self-posting). The routing key will
     represent the highest 24 bits of the MD5 digest of the (debtor
     ID, creditor ID) pair. For example, if debtor ID is equal to 123,
     and creditor ID is equal to 456, the routing key will be
     "0.0.0.0.1.0.0.0.0.1.0.0.0.1.0.0.0.0.1.1.0.1.0.0". This allows
     different accounts to be located on different database servers
     (sharding).

   **Note:** If you execute the "configure" command (see below), with
   the environment variable `SETUP_RABBITMQ_BINDINGS` set to `yes`, an
   attempt will be made to automatically setup all the required
   RabbitMQ queues, exchanges, and the bindings between them. However,
   this works only for the most basic setup.


Configuration
-------------

The behavior of the running container can be tuned with environment
variables. Here are the most important settings with some random
example values:

```shell
# TCP port for the STOMP server. The default is 1234.
SWPT_SERVER_PORT=1234

# A path to a server certificate PEM file. The certificate will
# be used (by both the client and the server) to authenticate
# before peer nodes. The default is "/etc/swpt/server.crt".
SWPT_SERVER_CERT=/etc/swpt/server.crt

# A path to a PEM file containing an *unencrypted* private key.
# The key will be used (by both the client and the server) to
# authenticate before peer nodes. The default
# is "/secrets/swpt-server.key".
SWPT_SERVER_KEY=/secrets/swpt-server.key

# The maximum number of messages that the STOMP server is allowed
# to store in memory. The default is 100.
SWPT_SERVER_BUFFER=100

# The maximum number of messages that the STOMP clinet is allowed
# to store in memory. The default is 100.
SWPT_CLIENT_BUFFER=100

# URL of the database that contains current node's data,
# including information about peer nodes. Currently, only
# the "file://" scheme is supported for the URL. The default
# is "file:///var/lib/swpt-nodedata".
SWPT_NODEDATA_URL=file:///var/lib/swpt-nodedata

# The URL for initiating connections with the RabbitMQ server
# which is responsible for brokering SMP messages. The default
# is "amqp://guest:guest@localhost:5672".
PROTOCOL_BROKER_URL=amqp://guest:guest@localhost:5672

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](../master/development.env) file.


Available commands
------------------

The [entrypoint](../master/docker/entrypoint.sh) of the docker
container allows you to execute the following *documented commands*:

* `swpt-server`

  Starts a STOMP server for a Swaptacular node. You can start simultaneously
  as many servers as you like.

* `swpt-client`

  Initiate a client STOMP connection to a peer Swaptacular node. A peer node
  ID, and a queue name should be specified as arguments. You can start
  simultaneously as many clients as you like.

This [docker-compose example](../master/docker-compose-all.yml) shows
how to use the generated docker image, along with the PostgerSQL
server, and the RabbitMQ server.


How to run the tests
--------------------

1.  Install [Docker Engine] and [Docker Compose].

2.  To create an *.env* file with reasonable defalut values, run this
    command:

        $ cp development.env .env

3.  To run the tests, use the following commands:

        $ docker-compose build
        $ docker-compose run tests-dummy test


How to setup a development environment
--------------------------------------

1.  Install [Poetry].

2.  Create a new [Python] virtual environment and activate it.

3.  To install dependencies, run this command:

        $ poetry install

4.  You can use `swpt-server` and `swpt-client` to run the server or the
    client, and `pytest --cov=swpt_stomp --cov-report=html` to run the tests
    and generate a test coverage report.

    Note however, that the above commands rely on being able to connect to a
    RabbitMQ server instance at "amqp://guest:guest@localhost:5672". Also,
    note that because the RabbitMQ "guest" user [can only connect from
    localhost], you should either explicitly allow the "guest" user to
    connect from anywhere, or create a new RabbitMQ user, and change the
    RabbitMQ connection URL accordingly (`PROTOCOL_BROKER_URL` in the *.env*
    file).



[Swaptacular]: https://swaptacular.github.io/overview
[STOMP protocol]: https://stomp.github.io/
[JSON serialization for the Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol-json.rst
[Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol.rst
[docker image]: https://www.geeksforgeeks.org/what-is-docker-images/
[PostgreSQL]: https://www.postgresql.org/
[RabbitMQ]: https://www.rabbitmq.com/
[RabbitMQ queue]: https://www.cloudamqp.com/blog/part1-rabbitmq-for-beginners-what-is-rabbitmq.html
[RabbitMQ exchanges]: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html
[Docker Engine]: https://docs.docker.com/engine/
[Docker Compose]: https://docs.docker.com/compose/
[Poetry]: https://poetry.eustace.io/docs/
[Python]: https://docs.python.org/
[can only connect from localhost]: https://www.rabbitmq.com/access-control.html#loopback-users
