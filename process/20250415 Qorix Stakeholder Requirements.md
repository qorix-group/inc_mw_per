# Overall goals

## Reuse of application software via managed APIs
  * Requirement: stkh_req__overall_goals__reuse_of_app_soft
  * Coverage: CFT ensures

## Enable cooperation via standardized APIs
  * Requirement: stkh_req__overall_goals__enable_cooperation
  * Coverage: CFT ensures

## Variant management
  * Requirement: stkh_req__overall_goals__variant_management
  * Coverage: CFT ensures; also versioning & backwards compat as discussed in the first workshop

## IP protection
  * Requirement: stkh_req__overall_goals__ip_protection
  * Coverage: Taken care of by developers and also license allows for this

# Functional requirements

## File Based Configuration
  * Requirement: stkh_req__functional_req__file_based
  * Coverage: Can be applied to the KVS builder pattern to configure memory boundaries etc

## Support of safe Key/Value store
  * Requirement: stkh_req__functiona_req__support_of_store
  * Coverage: Check

## Safe Configuration
  * Requirement: stkh_req__functional_req__safe_config
  * Coverage: Maybe not directly applicable for KVS.

## Safe Computation
  * Requirement: stkh_req__functional_req__safe_comput
  * Coverage: Maybe not directly applicable for KVS. Checksum?

## Hardware Accelerated Computation
  * Requirement: stkh_req__functional_req__hardware_comput
  * Coverage: Maybe not directly applicable for KVS. Checksum?

## Data Persistency
  * Requirement: stkh_req__functional_req__data_persistency
  * Coverage: Check

## Operating System
  * Requirement: stkh_req__functional_req__operating_system
  * Coverage: Maybe not directly applicable for KVS.

## Video subsystem
  * Requirement: stkh_req__functional_req__video_subsystem
  * Coverage: Not applicable for KVS.

## Compute subsystem
  * Requirement: stkh_req__functional_req__comp_subsystem
  * Coverage: Not applicable for KVS.

## Communication with external MCUs/standby controllers
  * Requirement: stkh_req__functional_req__comm_with_control
  * Coverage: Maybe applicable for KVS to share the store through shared memory or network protocols during runtime.

# Dependability

## Automotive Safety Integrity Level
  * Requirement: stkh_req__dependability__automotive_safety
  * Coverage: Check

## Safety features
  * Requirement: stkh_req__dependability__safety_features
  * Coverage: Maybe "Safe reset paths" and "Safe switch from engineering for field mode and back".

## Availability
  * Requirement: stkh_req__dependability__availability
  * Coverage: Maybe by using non-blocking code-components in the future.

## Security features
  * Requirement: stkh_req__dependability__security_features
  * Coverage: Maybe "Mandatory access control" and "Identity and Access Management".

# Application architectures

## Support for Time-based Architectures
  * Requirement: stkh_req__app_architectures__support_time
  * Coverage: Not applicable to KVS.

## Support for Data-driven Architecture
  * Requirement: stkh_req__app_architectures__support_data
  * Coverage: Maybe through callbacks.

## Support for Request-driven Architecture
  * Requirement: stkh_req__app_architectures__support_request
  * Coverage: If a request means "get", "set", etc. and Rust Async fulfills the asynchronous application architecture requirement, then yes.

# Execution model

## Processes and thread management
  * Requirement: stkh_req__execution_model__processes
  * Coverage: Not applicable for KVS.

## Short application cycles
  * Requirement: stkh_req__execution_model__short_app_cycles
  * Coverage: KVS without locking mechanism is only a memory map (without sync taken into account) and should be really fast. For shared KVS this will be taken into account later on by using a non-blocking architecture where possible.

## Realtime capabilities
  * Requirement: stkh_req__execution_model__realtime_cap
  * Coverage: Not applicable for KVS -- or similar to "Short application cycles"

## Startup performance
  * Requirement: stkh_req__execution_model__startup_perf
  * Coverage: Will be considered.

## Low power mode
  * Requirement: stkh_req__execution_model__low_power
  * Coverage: Not directly applicable to KVS.

# Communication

## Inter-process Communication
  * Requirement: stkh_req__communication__inter_process
  * Coverage: Can be taken into account for future versions.

## Intra-process Communication
  * Requirement: stkh_req__communication__intra_process
  * Coverage: Check

## Stable application interfaces
  * Requirement: stkh_req__communication__stable_app_inter
  * Coverage: Ensured by the CFT

## Extensible External Communication
  * Requirement: stkh_req__communication__extensible_external
  * Coverage: Can be implemented on future versions.

## Safe Communication
  * Requirement: stkh_req__communication__safe
  * Coverage: Can be later implemented by using a provided API or tunnel for this.

## Secure Communication
  * Requirement: stkh_req__communication__secure
  * Coverage: Maybe later.

## Supported network protocols
  * Requirement: stkh_req__communication__supported_net
  * Coverage: Not directly applicable for KVS.

## Quality of service
  * Requirement: stkh_req__communication__service_quality
  * Coverage: Currently not applicable for KVS, but maybe later.

## Automotive diagnostics
  * Requirement: stkh_req__communication__auto_diagnostics
  * Coverage: Must be implemented by the components which provide the KVS content as needed.

# Hardware support

## Chipset support for ARM64 and x64
  * Requirement: stkh_req__hardware_support__chipset_support
  * Coverage: Provided by Rust.

## Virtualization support for debug and testing
  * Requirement: stkh_req__hardware_support__debug_and_test
  * Coverage: Not in scope of the KVS.

## Support of container technologies
  * Requirement: stkh_req__hardware_support__container_tech
  * Coverage: Not in scope of the KVS.

# Developer experience

## IDL Support
  * Requirement: stkh_req__dev_experience__idl_support
  * Coverage: Not in scope of the KVS.

## Developer experience and development toolchain
  * Requirement: stkh_req__dev_experience__dev_toolchain
  * Coverage: Not in scope of the KVS but allows to show the JSON storage format nicely
formatted through IDE plugins.

## Performance analysis
  * Requirement: stkh_req__dev_experience__perf_analysis
  * Coverage: Not in scope of the KVS.

## Tracing of execution
  * Requirement: stkh_req__dev_experience__tracing_of_exec
  * Coverage: Not in scope of the KVS.

## Tracing of communication
  * Requirement: stkh_req__dev_experience__tracing_of_comm
  * Coverage: Not in scope of the KVS.

## Tracing of memory access
  * Requirement: stkh_req__dev_experience__tracing_of_memory
  * Coverage: Not in scope of the KVS.

## Timing analysis
  * Requirement: stkh_req__dev_experience__timing_analysis
  * Coverage: Not in scope of the KVS.

## Debugging
  * Requirement: stkh_req__dev_experience__debugging
  * Coverage: Not in scope of the KVS.

## Programming languages for application development
  * Requirement: stkh_req__dev_experience__prog_languages
  * Coverage: Partly covered by using Rust for the KVS implementation.

## Reprocessing and simulation support
  * Requirement: stkh_req__dev_experience__reprocessing
  * Coverage: Maybe covered through the snapshot functionality of the KVS.

## Logging support
  * Requirement: stkh_req__dev_experience__logging_support
  * Coverage: Not in scope of the KVS.

## Previous boot logging
  * Requirement: stkh_req__dev_experience__boot_logging
  * Coverage: Not in scope of the KVS.

# Integration

## Multirepo integration
  * Requirement: stkh_req__integration__multi_repo
  * Coverage: Not in scope of the KVS.

# Quality

## Document assumptions and design decisions
  * Requirement: stkh_req__quality__assumptions_and_dd
  * Coverage: Is already covered by the CFT process of the KVS.

# Requirements Engineering

## Requirements traceability
  * Requirement: stkh_req__re_requirements__traceability
  * Coverage: Not in scope of the KVS.

## Document requirements as code
  * Requirement: stkh_req__requirements__as_code
  * Coverage: Will be covered by the KVS.
