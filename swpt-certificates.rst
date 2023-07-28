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


Certificate types
=================

There are 3 types of certificates:

root certificates
  Every Swaptacular node issues a self-signed certificate to itself. The
  expiration date of root certificates SHOULD be set very far in the future
  (after 500 years for example).

  Root certificates MUST include the following extensions:

  - *Basic constraints* extension, marked as "critical", with its ``cA``
    boolean field set to ``true``, and its ``pathLenConstraint`` field set
    to at least ``1``, or not set at all. This ensures that the certificate
    can participate in the chain of trust.

  - *Key usage* extension, marked as "critical" with its ``keyCertSign`` bit
    set to ``true``. This ensures that the public key can be used for
    verifying signatures on peer certificates.

  - "Subject Key Identifier" extension, specifying the SHA-1 hash of the
    subject's public key as key identifier. This provides a means to quickly
    identify the set of certificates containing a particular public key.

server certificates
  Server certificates are used to authenticate Swaptacular nodes' servers
  before their peers. Swaptacular nodes may issue as many different server
  certificates as they want, but the subject of each server certificate MUST
  be exactly the same as the subject of the node's root certificate. Also,
  each server certificate MUST be signed using the node's root certificate.
  The expiration date of server certificates SHOULD be set in the relatively
  close future (after 1 year for example).

  Server certificates MUST include the following extensions:
  
  - *Basic constraints* extension, marked as "critical", with its ``cA``
    boolean field set to ``false``. This ensures that the certificate can
    only appear at the end of the chain of trust.

  - *Key usage* extension, marked as "critical" with its
    ``digitalSignature`` and ``keyEncipherment`` bits set to ``true``. This
    ensures that the stated key usage is consistent with the purposes listed
    in the "Extended key usage" extension.

  - *Extended key usage* extension, marked as "non-critical", with its
    allowed purposes including ``clientAuth`` and ``serverAuth``. This
    ensures that the certificate can be used for client and server
    authentication.

  - *Subject Key Identifier* extension, marked as "non-critical", specifying
    the SHA-1 hash of the server's public key as key identifier. This
    provides a means to quickly identify the set of certificates containing
    a particular public key.

  - *Authority Key Identifier* extension, marked as "non-critical",
    specifying the SHA-1 hash of the issuer's public key as key identifier.
    This provides a means of identifying the public key corresponding to the
    private key used to sign the certificate.

peer certificates
  Peer certificates are issued to peers nodes, so that peers can prove their
  identity before your servers. A peer certificate is issued for each peer
  node. Peer certificates MUST be signed using the node's root certificate.
  The expiration date of peer certificates SHOULD be set very far in the
  future (after 500 years for example).

  Peer certificates MUST include the following extensions:

  - *Basic constraints* extension, marked as "critical", with its ``cA``
    boolean field set to ``true``, and its ``pathLenConstraint`` field set
    to at least ``0``, or not set at all. This ensures that the certificate
    can participate in the chain of trust.

  - *Key usage* extension, marked as "critical" with its ``keyCertSign`` bit
    set to ``true``. This ensures that the public key can be used for
    verifying signatures on server certificates.

  - *Name constraints* extension, marked as "critical", specifying in its
    ``permittedSubtrees`` field a restriction of the ``directoryName`` form,
    ensuring that all certificates down the trust chain can not change
    subject's ``O``, ``OU``, and ``serialNumber`` distinguished name
    attributes.

  - *Subject Key Identifier* extension, marked as "non-critical", specifying
    the SHA-1 hash of the peer's public key as key identifier. This provides
    a means to quickly identify the set of certificates containing a
    particular public key.

  - *Authority Key Identifier* extension, marked as "non-critical",
    specifying the SHA-1 hash of the issuer's public key as key identifier.
    This provides a means of identifying the public key corresponding to the
    private key used to sign the certificate.

  - *Netscape certificate comment* (OID value: 2.16.840.1.113730.1.13),
    marked as "non-critical", ...


Certificate Subject's Distinguished Name
========================================

TODO

1. The "Organization" (``O``) MUST be ``Swaptacular Nodes Registry``.

2. The "Organizational Unit" (``OU``) MUST be:

   - ``Accounting Authorities`` for accounting authority nodes;
   - ``Creditors Agents`` for creditors agent nodes;
   - ``Debtors Agents`` for debtors agent nodes.

3. The "Serial Number" (``serialNumber``) MUST be...



.. _X509: https://datatracker.ietf.org/doc/html/rfc5280
