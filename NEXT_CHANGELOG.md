# NEXT CHANGELOG

## [Unreleased]

### Added
- **Query Tags support**: Added ability to attach key-value tags to SQL queries for analytical purposes that would appear in `system.query.history` table. Example: `jdbc:databricks://host;QUERY_TAGS=team:marketing,dashboard:abc123`. 

### Updated

### Fixed
- Fixed `DatabricksUCVolumeClient` delete to skip file path validation and remove redundant dependency on `VolumeOperationAllowedLocalPaths`.
---
*Note: When making changes, please add your change under the appropriate section with a brief description.* 
