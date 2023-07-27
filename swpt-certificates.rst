++++++++++++++++++++++++++++++++
Swaptacular SSL/TLS Certificates
++++++++++++++++++++++++++++++++
:Description: Specifies the way Swaptacular Messaging Protocol
              messages should be serialized to JSON
:Author: Evgeni Pandurksi
:Contact: epandurski@gmail.com
:Date: 2023-08-01
:Version: 1.0
:Copyright: This document has been placed in the public domain.


Overview
========

This document specifies how Swaptacular Messaging Protocol messages
are serialized to JSON documents (``"applicatoin/json"``).

**Note:** The key words "MUST", "MUST NOT", "REQUIRED", "SHALL",
"SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in
RFC 2119.


Certificate types
=================

There are 3 types of certificates:

root certificates
  Every Swaptacular issues a self-signed certificate to itself.
  
  The expiration date of root certificates SHOULD be set very far in the
  future (500 years for example).

server certificates
  They are used by Swaptacular nodes' servers, so that the server can prove
  their identity before Swaptacular peer nodes.

  Node can issue many server certificates, which MUST be signed using the
  node's root certificate. The subject's DN for server certificates MUST be
  the same as for the root certificate.

  The expiration date of root certificates SHOULD NOT be set too far in the
  future (1 year for example).

peer certificates
  Are issued to peers nodes, so that peer nodes can prove their identity
  before your servers.

  Each peer certificate MUST include:

  - "basicConstraints" extension, marked as "critical", with its "CA" field
    set to `true`. This ensures that the certificate can participate in a
    chain of trust.
    
  - "keyUsage" extension, marked as "critical" with its "keyCertSign" bit
    set to `true`.

  - "nameConstraints" extension, marked as "critical", specifying in its
    "permittedSubtrees" field a restriction of the `directoryName` form
    (That is: ensuring that certificates down the chain can not change
    subject's `O`, `OU`, and `serialNumber` DN attributes).
    
  - "Subject Key Identifier" extension, specifying the SHA-1 hash of the
    subject's public key as key identifier.

  A peer certificate is issued for each peer. Peer certificates MUST be
  signed using the node's root certificate. The expiration date of peer
  certificates SHOULD be set very far in the future (500 years for example).


Certificate Subject's Distinguished Name
========================================

1. The "Organization" (`O`) MUST be `Swaptacular Nodes Registry`.

2. The "Organizational Unit" (`OU`) MUST be:

   - `Accounting Authorities` for accounting authority nodes;
   - `Creditors Agents` for creditors agent nodes;
   - `Debtors Agents` for debtors agent nodes.

3. The "Serial Number" (`serialNumber`) MUST be...

   
Certificate Expiration Date
===========================

The expiration date of peer certificates SHOULD be set very far in the
future (500 years for example).


.. _X509: https://datatracker.ietf.org/doc/html/rfc5280






Required Message Fields
=======================

For every specific type of message defined by the Swaptacular
Messaging Protocol's specification, all of the defined message fields
MUST be present in the serialized JSON document as properties. In
addition, a ``"type"`` property MUST exist, specifying the type of the
message.

For example, the serialization of an ``AccountPurge`` message would
look like this::

  {
    "type": "AccountPurge",
    "debtor_id": 1234,
    "creditor_id": 5678,
    "creation_date": "2022-08-19",
    "ts": "2022-08-20T16:59:59Z"
  }


Message Filed Types
===================

The specification of the Swaptacular Messaging Protocol uses several
different field types, which MUST be serialized to JSON values as
follows:


int32
  To JSON number.

  MUST be formatted as integer. MUST NOT contain a decimal point
  (``.``) or an exponent (``e`` or ``E``).


int64
  To JSON number.

  MUST be formatted as integer. MUST NOT contain a decimal point
  (``.``) or an exponent (``e`` or ``E``).

  **Note for implementators:** Even thought ECMAScript 2021 supports
  `BigInt`s, the standard JSON parser and serializer does not allow to
  correctly process numbers outside the safe range from ```-(2 ** 53 -
  1)`` to ``2 ** 53 - 1``.
  
float  
  To JSON number.

  MUST be formatted as floating point number. MUST contain a decimal
  point (``.``), or an exponent (``e`` or ``E``), or both. The reason
  for this requirement is to allow generic JSON parsers to easily
  differentiate integers from floats.

  **Note for implementators:** The standard ECMAScript 2021 JSON
  serializer does not satisfy this requirement.

string
  To JSON string.

  Non-ASCII characters SHOULD NOT be escaped using the ``\uXXXX``
  syntax.

date-time  
  To JSON string.

  The ISO 8601 timestamp format MUST be used.
  
date
  To JSON string.

  The ISO 8601 date format MUST be used (``YYYY-MM-DD``).

bytes
  To JSON string.
  
  Each byte MUST be represented by exactly two hexadecimal *uppercase*
  characters (Base16 encoding).
  
  
Default Encoding
================

When messages are serialized in JSON format, and received as a
byte-stream, without an explicitly prescribed encoding, UTF-8 encoding
MUST be presumed.
