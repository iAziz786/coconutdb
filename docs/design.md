# CoconutDB - Design

User flow for the coconutdb will first inspire by the famous open source library in Go community as a key-value store, bbolt.

## Key Components

The bbolt lib has many key components which leads to entirety of the storage provider. This part of the document explains what key components are used in it so that it's easy to port to its Rust version.

### Cursor

All the CRUD like operations which happen in bbolt it happens through an abstraction called cursor. A cursor is a pointer
