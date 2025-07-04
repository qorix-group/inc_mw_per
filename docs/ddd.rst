Detailed Design for Component: rust_kvs
=======================================

Description
-----------

This document showcases the integration between Rust code and Sphinx documentation using the `sphinxcontrib-rust` extension.  
The `rust_kvs` crate provides a key-value storage solution in Rust with JSON-like persistence and is documented directly from Rust source code.  
This approach **ensures traceability** and minimizes manual documentation drift.

| **Design Decisions**
| - Leverage code-level documentation for API docs (minimize manual work).
| - Public Rust API is always reflected in documentation.
| - Full traceability from requirements to code and API.

| **Design Constraints**
| - Only public API is documented (as per current extension configuration).
| - The extension requires a buildable and parseable Rust crate structure.

Rationale Behind Decomposition into Units
*****************************************

The decomposition into units is guided by:
- **SOLID** principles for maintainability and separation of concerns.
- **API boundary clarity**: structs, enums, and traits each define a logical design unit.
- Rust module boundaries.

Static Diagrams for Unit Interactions
-------------------------------------

.. dd_sta:: Key Rust API Structure
   :id: dd_sta__ddd__api
   :security: YES
   :safety: ASIL_B
   :status: valid
   :implements: comp_req__rust_kvs
   :satisfies: comp_arc_sta__rust_kvs

.. uml::
   :caption: Example module structure

   @startuml
   package "rust_kvs" {
       class Kvs
       class KvsApi <<trait>>
       class KvsBuilder
       class KvsValue
       class ErrorCode
   }
   Kvs -|> KvsApi
   KvsBuilder --> Kvs
   @enduml

Dynamic Diagrams for Unit Interactions
--------------------------------------

.. dd_dyn:: Typical Usage
   :id: dd_dyn__ddd__usage
   :security: NO
   :safety: QM
   :status: valid
   :implements: comp_req__rust_kvs
   :satisfies: comp_arc_sta__rust_kvs

.. uml::
   :caption: Typical Use Flow

   @startuml
   actor User
   User -> KvsBuilder: new()
   KvsBuilder -> KvsBuilder: need_defaults()/need_kvs()
   KvsBuilder -> KvsBuilder: build()
   KvsBuilder --> Kvs: returns Kvs instance
   User -> Kvs: set_value()/get_value()
   User -> Kvs: flush()
   @enduml

Units within the Component
--------------------------

.. sw_unit:: Kvs
   :id: sw_unit__ddd__kvs
   :security: YES
   :safety: ASIL_B
   :status: valid

.. rust:struct:: rust_kvs::Kvs
   :index: 1
   :vis: pub

   Key-value-storage data (see API below, auto-generated)

.. sw_unit:: KvsApi
   :id: sw_unit__ddd__kvsapi
   :security: YES
   :safety: ASIL_B
   :status: valid

.. rust:trait:: rust_kvs::KvsApi
   :index: 1
   :vis: pub

   The public API for interacting with the KVS (auto-generated).

.. sw_unit:: ErrorCode
   :id: sw_unit__ddd__errorcode
   :security: YES
   :safety: ASIL_B
   :status: valid

.. rust:enum:: rust_kvs::ErrorCode
   :index: 1
   :vis: pub

   Error codes for runtime failures (auto-generated).

Interface
*********

.. sw_unit_int:: Kvs Public Interface
   :id: sw_unit_int__ddd__kvs_public
   :security: YES
   :safety: ASIL_B
   :status: valid

Auto-generated API reference (see below):

.. toctree::
   :maxdepth: 2

   crates/rust_kvs/lib

Key Features Demonstrated
-------------------------

- **Automatic extraction of all structs, enums, traits, and their documentation.**
- **API documentation always in sync with the Rust codebase.**
- **Cross-links between Rust types, trait implementations, and function signatures.**
- **Integration of code-level docs with Sphinx diagrams and requirements traceability.**
- **Supports documenting multiple crates in a single project.**

.. note::
   All Rust API documentation is auto-generated from code using the `sphinxcontrib-rust` extension and `sphinx-rustdocgen`.
   To update docs, simply update code comments and re-build the documentation.
