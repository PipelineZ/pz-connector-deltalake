# Azure Blob and ADLS credentials

An `az://`, `abfs://`, `abfss://` or `adl://` root turns on the Azure half of the connection block.

**Coverage first, because it is uneven.** Azure `append` and reads are proven against the Azurite
emulator. **`replace`, `merge` and concurrent commits on Azure are not proven and are not claimed**,
and no test in this repository has ever talked to real Azure Storage. `abfss://`-family roots are
accepted by the connector's root classification and unit-tested there — nothing has ever written a
table through one. [../compatibility.md](../compatibility.md) is the full matrix; read it before
depending on any of this.

## Five mechanisms, tried in order

The connector picks **one** mechanism and gives the same one to both engines. It does not emit every
configured key at once: with a partial config — a connection string plus stray tenant/client fields
left over from an earlier edit — DuckDB and delta-rs could otherwise pick different mechanisms, and a
read and a schema probe of the same dataset would authenticate two different ways.

### 1. A connection string

```yaml
lake:
  connector: deltalake
  root: az://curated/lake
  connection_string: ${AZURE_STORAGE_CONNECTION_STRING}
```

DuckDB takes it whole. delta-rs has no storage-option key for a connection string at all, so the
connector decomposes it into the keys delta-rs does read: `AccountName`, `AccountKey`,
`SharedAccessSignature` and `BlobEndpoint`. A `SharedAccessSignature` field is how you use a SAS
token here.

### 2. A service principal

```yaml
lake:
  connector: deltalake
  root: az://curated/lake
  account_name: mystorageaccount
  tenant_id: ${AZURE_TENANT_ID}
  client_id: ${AZURE_CLIENT_ID}
  client_secret: ${AZURE_CLIENT_SECRET}
```

All four are needed together; three of them fall through to the next mechanism.

### 3. Account name and shared key

```yaml
lake:
  connector: deltalake
  root: az://curated/lake
  account_name: mystorageaccount
  account_key: ${AZURE_STORAGE_KEY}
```

### 4. A user-assigned managed identity

```yaml
lake:
  connector: deltalake
  root: az://curated/lake
  account_name: mystorageaccount
  client_id: ${AZURE_CLIENT_ID}
```

`client_id` with no `tenant_id` and no `client_secret` picks one user-assigned managed identity by
its client id. Use this whenever the host has more than one identity: an Azure Container Apps job
with two user-assigned identities fails the credential chain with `Failed to get token from
ChainedTokenCredential`, because the chain cannot tell which one you mean.

On Container Apps and App Service the token comes from the endpoint in `IDENTITY_ENDPOINT`, not the
VM metadata address. The connector forwards it to delta-rs for you; DuckDB finds it on its own.

### 5. Account name alone

```yaml
lake:
  connector: deltalake
  root: az://curated/lake
  account_name: mystorageaccount
```

Managed identity, or whatever the ambient Azure credential chain resolves. With several identities on
the host, use mechanism 4 instead.

## Linux hosts

DuckDB's azure extension, by default, cannot find the system CA bundle on Linux (seen on Ubuntu 24.04
Container Apps and Arch): every request fails before it leaves the host, and the error reads like a
credentials problem (`Fail to get a new connection for: https://<account>.blob.core.windows.net`). On
Linux the connector therefore sets `azure_transport_option_type = 'curl'`, which uses the system
bundle. Azurite never shows the problem, because it is plain http.

Where that identity is announced through environment variables (Container Apps and App Service set
`IDENTITY_ENDPOINT`/`IDENTITY_HEADER`, AKS workload identity sets `AZURE_FEDERATED_TOKEN_FILE`, a
service principal sets `AZURE_CLIENT_SECRET`), the connector process only sees them with pz 0.14.0 or
newer: pz starts external connectors with a cleared environment and forwards the Azure identity
variables to this package because it declares them. An Azure VM's metadata endpoint needs no variable
and works with any pz.

## Plain http endpoints

The object-store client refuses a plain-http endpoint unless told to allow one, and the refusal names
nothing you can act on — measured against a real http `BlobEndpoint`, every request fails with
`Generic MicrosoftAzure error: ... HTTP error: builder error`, which says neither "http" nor
"endpoint".

So the connector reads the scheme you wrote into `BlobEndpoint` and opts in for you. There is no
`use_ssl` option on an Azure root (it is S3-only, and declaring it is PZDL0102): the connection string
decides. `https` endpoints — which is every real Azure account — need nothing.

## Developing against Azurite

Two limits, both measured, both **artifacts of the emulator's addressing rather than properties of
the Azure write path**:

- **One commit per table.** The first write succeeds; every later one fails with
  `Invalid table version: N`. Azurite puts the storage account in the URL **path**
  (`http://host:port/devstoreaccount1`); a real account carries it in the **host** and has no path,
  which is the shape the object-store client is written for. The proof that it is addressing and not
  the write path: the same sequence of commits all succeed the moment the client is put into its own
  emulator mode.
- **DuckDB reads only on port 10000.** `delta_scan` reports a GET against `127.0.0.1:10000` whatever
  port the emulator is on — DuckDB's delta extension reads through delta-kernel-rs, whose object store
  recognizes an emulator connection string and resolves the well-known emulator port for it.

The connector does **not** switch to emulator mode, deliberately: that mode ignores the configured
endpoint and resolves `127.0.0.1:10000`, which would break every user running Azurite anywhere else.
To develop locally against Delta on Azure, use a real storage account, run Azurite on port 10000, or
accept one commit per table.
