# NEXT CHANGELOG

## [Unreleased]

### Added
- Added support for geospatial data types.
* Added support for telemetry log levels, which can be controlled via the connection parameter `TelemetryLogLevel`. This allows users to configure the verbosity of telemetry logging from OFF to TRACE.

### Updated
* Updated sdk version from 0.65.0 to 0.67.3

### Fixed
- Fixed SQL syntax error when LIKE queries contain empty ESCAPE clauses.
- Fix: driver failing to authenticate on token update in U2M flow.
- Fix: driver failing to parse complex data types with nullable attributes.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
