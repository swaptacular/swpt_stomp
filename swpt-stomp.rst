++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
STOMP Message Transport for the Swaptacular Messaging Protocol
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
:Description: Specifies how Swaptacular nodes use a subset of the STOMP
              protocol, to interoperably send SMP messages from one peer
              node to another.
:Author: Evgeni Pandurksi
:Contact: epandurski@gmail.com
:Date: 2023-07-10
:Version: 1.0
:Copyright: This document has been placed in the public domain.


Overview
========

This document specifies how Swaptacular nodes use a subset of the STOMP 1.2
protocol, to interoperably send Swaptacular Messaging Protocol messages from
one peer node to another.

**Note:** The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL
NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this
document are to be interpreted as described in RFC 2119.


STOMP Protocol
==============

  STOMP [#stomp]_ (Simple Text Oriented Messaging Protocol) is a simple
  interoperable protocol designed for asynchronous message passing between
  clients via mediating servers. It defines a text based wire-format for
  messages passed between these clients and servers.

Every Swaptacular node MUST run one or more publicly accessible servers,
allowing peer Swaptacular nodes to connect to these servers as clients, and
post messages. Servers and clients MUST support the following subset of the
STOMP 1.2 specification:

- The STOMP 1.2 commands ``STOMP``, ``CONNECT``, ``CONNECTED``, ``SEND``,
  ``RECEIPT``, ``ERROR``, and ``DISCONNECT`` **MUST be fully supported**.

- Support for STOMP 1.2 *subscriptions* and *transactions* is OPTIONAL. That
  is: ``SUBSCRIBE``, ``UNSUBSCRIBE``, ``MESSAGE``, ``ACK``, ``NACK``,
  ``BEGIN``, ``COMMIT``, and ``ABORT`` commands may not be implemented.
  Swaptacular nodes MUST NOT expect all peer nodes to understand these
  commands.

- In addition to the requirements given by the STOMP 1.2 specification,
  every ``SEND`` command MUST include the following headers:

   receipt
     Specifies a message ID.
     
   type
     Specifies the type of the SMP [#smp]_ message.

     Here is a non-exhaustive list of possible message types:
     - ``ConfigureAccount``
     - ``PrepareTransfer``
     - ``FinalizeTransfer``
     - ``RejectedConfig``
     - ``RejectedTransfer``
     - ``PreparedTransfer``
     - ``FinalizedTransfer``
     - ``AccountUpdate``
     - ``AccountPurge``
     - ``AccountTransfer``
        
   content-type
     Specifies the MIME type of the message body.

     Every Swaptacular node MUST support the JSON serialization format (the
     ``application/json`` MIME type), and MAY support additional
     serialization formats and MIME types.
     
   persistent
     MUST have the value ``true``.

In addition to the above described STOMP subset, servers and clients MAY
support other message transport protocols. If some other message transport
protocol is supported by both the server and the client, they MAY agree to
use it instead.

     
STOMP Connections
=================

When a Swaptacular node wants to send some SMP messages to a peer
Swaptacular node, the first node opens a client STOMP connection to the
second node's servers, and issues a ``SEND`` command for each of the
messages. The client MUST consider a message to be successfully delivered,
only after a ``RECEIPT`` command has been received from the server,
confirming that the message has been processed [#multiple-ack]_.

The client MAY decide to keep the STOMP connection open for any length of
time, and the server SHOULD NOT terminate the connection unilaterally,
without a reason.

STOMP connections between the client and the server MUST be secured by using
**Transport Layer Security** version 1.3 or higher. *Both the client and the
server* must present a certificate, which the other side can verify before
proceeding with the connection. That is:

- Clients MUST require servers to authenticate themselves by presenting a
  trusted certificate chain. Clients SHOULD NOT perform *hostname
  verification* [#host-check]_.

- Servers MUST require clients to authenticate themselves by presenting a
  trusted certificate chain.

.. [#smp] Swaptacular Messaging Protocol
     
.. [#stomp] https://stomp.github.io/

.. [#multiple-ack] Note that every STOMP ``RECEIPT`` command confirms the
  delivery of all preceding messages.

.. [#host-check] The *hostname verification* involves looking at the
  certificate sent by the server, and verifying that the ``dnsName`` in the
  ``subjectAltName`` field of the certificate matches the host portion of
  the URL used to make the connection.


STOMP Server Manifest Files
===========================

For a Swaptacular node to be able to automatically initiate connections to
peer nodes' STOMP servers, some basic information about the servers
(hostname, TCP port etc.) needs to be available in a machine-readable
format.

*STOMP Server Manifest Files* are TOML [#toml]_ files than contain the
following configuration keys:

servers
  A list of server addresses in the form ``"hostname:port"``.
  
  The ``hostname`` can be a fully qualified domain name, or an IP address;
  ``port`` specifies the TCP port that the servers listens on. To initiate a
  new connection, the client SHOULD randomly choose one of the server
  addresses from the list. Note that the list MAY contain the same server
  address more than once, which would increase the chances for that address
  to be chosen by clients.

host
  A value for the ``CONNECT`` [#connect]_ command's ``host`` header.

  The client MUST substitute all occurrences of the string ``${NODE_ID}`` in
  the value, with the ID of the client's Swaptacular node. For example, if
  the value is ``"/peer/${NODE_ID}"``, and the client's node ID is
  ``12345678``, then the client must send the header
  ``"host:/peer/12345678"`` with its ``CONNECT`` command to the server.

login  
  An *optional* value for the ``CONNECT`` command's ``login`` header.

  Servers SHOULD NOT require clients to include a ``login`` header (an
  username) with the ``CONNECT`` command.

  The client MUST substitute all occurrences of the string ``${NODE_ID}`` in
  the value, with the ID of the client's Swaptacular node.

passcode  
  An *optional* value for the ``CONNECT`` command's ``passcode`` header.

  Servers SHOULD NOT require clients to include a ``passcode`` header (a
  password) with the ``CONNECT`` command.

destination
  A value for the ``SEND`` command's ``destination`` header.

  The client MUST substitute all occurrences of the string ``${NODE_ID}`` in
  the value, with the ID of the client's Swaptacular node.

accepted-content-types
  TODO

For example::

  servers = ["server1.example.com:1234", "server2.example.com:1234"]
  host = "/"
  destination = "/smp/${NODE_ID}"
  accepted-content-types = [
    ""
  ]

.. [#toml] https://toml.io/en/

.. [#connect] The STOMP protocol specification requires servers to handle
  the ``STOMP`` command in the same manner as the ``CONNECT`` command.
  Therefore, everything said in this section applies to the ``STOMP``
  command as well.
