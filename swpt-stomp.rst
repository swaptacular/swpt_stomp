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
  ``RECEIPT``, ``ERROR``, and ``DISCONNECT`` MUST be fully supported.

- Support for STOMP 1.2 *subscriptions* and *transactions* is OPTIONAL. That
  is: ``SUBSCRIBE``, ``UNSUBSCRIBE``, ``MESSAGE``, ``ACK``, ``NACK``,
  ``BEGIN``, ``COMMIT``, and ``ABORT`` STOMP commands may not be
  implemented. Swaptacular nodes MUST NOT expect peer nodes to understand
  these commands.

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

In addition to the above described protocol, servers and clients MAY support
other message transport protocols. If some other message transport protocol
is supported by both the server and the client, they MAY agree to use it
instead.

     
STOMP Connections
=================

When a Swaptacular node needs to send some messages to a peer node, the
Swaptacular node opens a client STOMP connection to the peer node's servers,
and issues a ``SEND`` command for each of the messages. The client MUST
consider a sent message to be successfully delivered, only after a
``RECEIPT`` command has been received from the server, confirming that the
message has been processed [#multiple-ack]_.

The client MAY decide to keep the STOMP connection open for any length of
time, and the server SHOULD NOT terminate the connection unilaterally,
without a reason.

.. [#smp] Swaptacular Messaging Protocol
     
.. [#stomp] https://stomp.github.io/

.. [#multiple-ack] Note that every STOMP ``RECEIPT`` command confirms the
  delivery of all preceding messages.
