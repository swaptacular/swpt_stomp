++++++++++++++++++++++++++++++++
Swaptacular SSL/TLS Certificates
++++++++++++++++++++++++++++++++
:Description: Specifies the different types of SSL/TLS certificates used in
              Swaptacular.
:Author: Evgeni Pandurksi
:Contact: epandurski@gmail.com
:Date: 2023-08-01
:Version: 1.0
:Copyright: This document has been placed in the public domain.


Overview
========

This document specifies the different types of SSL/TLS certificates used in
Swaptacular.

**Note:** The key words "MUST", "MUST NOT", "REQUIRED", "SHALL",
"SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in
RFC 2119.


Root certificates
=================

Every Swaptacular node issues a self-signed certificate to itself. The
expiration date of root certificates SHOULD be set very far in the future
(after 500 years for example).

Root certificates MUST include the following standard certificate
extensions:

- *Basic Constraints* extension, marked as "critical", with its ``cA``
  boolean field set to ``true``, and its ``pathLenConstraint`` field set to
  at least ``1``, or not set at all. This ensures that the certificate can
  participate in the chain of trust.

- *Key Usage* extension, marked as "critical" with its ``keyCertSign`` bit
  set to ``true``. This ensures that the public key can be used for
  verifying signatures on other certificates.

- *Subject Key Identifier* extension, specifying the SHA-1 hash of the root
  certificate's public key as key identifier. This provides a means to
  quickly identify the set of certificates containing a particular public
  key.


Server certificates
===================

Server certificates are used to authenticate Swaptacular nodes' servers
before their peer nodes' servers. Server certificates MUST be signed with
the private key used for signing the node's root certificate. Swaptacular
nodes may issue as many server certificates as they want, but the subject of
each server certificate MUST be exactly the same as the subject of the
node's root certificate. The expiration date of server certificates SHOULD
be set in the relatively close future (after 1 year for example).

Server certificates MUST include the following standard certificate
extensions:

- *Basic Constraints* extension, marked as "critical", with its ``cA``
  boolean field set to ``false``. This ensures that the certificate can only
  appear at the end of the chain of trust.

- *Key Usage* extension, marked as "critical" with its ``digitalSignature``
  and ``keyEncipherment`` bits set to ``true``. This ensures that the stated
  key usage is consistent with the purposes listed in the "Extended Key
  Usage" extension.

- *Extended Key Usage* extension, marked as "non-critical", listing
  ``clientAuth`` and ``serverAuth`` as allowed purposes. This ensures that
  the certificate can be used for client and server authentication.

- *Subject Key Identifier* extension, marked as "non-critical", specifying
  the SHA-1 hash of the server public key as key identifier. This provides a
  means to quickly identify the set of certificates containing a particular
  public key.

- *Authority Key Identifier* extension, marked as "non-critical", specifying
  the SHA-1 hash of the root certificate's public key as key identifier.
  This provides a means of identifying the public key corresponding to the
  private key used to sign the certificate.


Peer certificates
=================

Peer certificates are issued to peers nodes, so that peers nodes' servers
can present them, along with a server certificate, to prove their identity
(forming a verifiable chain of trust). A peer certificate should be issued
for each peer node. Peer certificates must be signed with the private key
used for signing the node's root certificate. The expiration date of peer
certificates SHOULD be set very far in the future (after 500 years for
example).

For every peer certificate, the subject and the subject's public key MUST
exactly match those of the corresponding peer's root certificate. Similarly,
the issuer and the issuer's public key MUST exactly match those of the
issuing node's root certificate.

Peer certificates MUST include the following standard certificate
extensions:

- *Basic Constraints* extension, marked as "critical", with its ``cA``
  boolean field set to ``true``. This ensures that the certificate can
  participate in the chain of trust.

- *Key Usage* extension, marked as "critical" with its ``keyCertSign`` bit
  set to ``true``. This ensures that the public key can be used for
  verifying signatures on other certificates.

- *Name Constraints* extension, marked as "critical", specifying in its
  ``permittedSubtrees`` field a restriction of the ``directoryName`` form,
  ensuring that all certificates down the trust chain will not change the
  subject's ``O``, ``OU``, and ``serialNumber`` distinguished name
  attributes.

- *Subject Key Identifier* extension, marked as "non-critical", specifying
  the SHA-1 hash of the peer's public key as key identifier. This provides a
  means to quickly identify the set of certificates containing a particular
  public key.

- *Authority Key Identifier* extension, marked as "non-critical", specifying
  the SHA-1 hash of the root certificate's public key as key identifier.
  This provides a means of identifying the public key corresponding to the
  private key used to sign the certificate.

Peer certificates issued by *accounting authority nodes*, in addition to the
previously listed extensions, MUST include the *Netscape Certificate
Comment* extension (OID value: 2.16.840.1.113730.1.13), marked as
"non-critical". The content MUST match the following regular expression::

  Subnet: [a-f0-9]{6,16}

The hexadecimal value after the "Subnet: " prefix specify the range of
creditor/debtor IDs (depending on the peer node's type) which the peer node
is responsible for. For example, if "Subnet: 123abc" is given to a
*creditors agent node*, the peer node will be responsible for managing all
creditor IDs between ``0x123abc0000000000`` and ``0x123abcffffffffff``
inclusive.


Subject distinguished names
===========================

TODO

1. The "Organization" (``O``) MUST be ``Swaptacular Nodes Registry``.

2. The "Organizational Unit" (``OU``) MUST be:

   - ``Accounting Authorities`` for accounting authority nodes;
   - ``Creditors Agents`` for creditors agent nodes;
   - ``Debtors Agents`` for debtors agent nodes.

3. The "Serial Number" (``serialNumber``) MUST be...



.. _X509: https://datatracker.ietf.org/doc/html/rfc5280
