# Changelog

All notable changes to this project will be documented in this file.

## [2.0.4]

### Added

- Added Certificate Auth (mTLS) support for Couchbase
- Added the ability to provide the CA certificate for Couchbase (which enables cert validation)

### Changed

- Changed the scan method logic in the Couchbase driver

### Fixed

- Fixed issue where Couchbase Workload E would not log errors
- Fixed issue with non-default scopes and collections

### Removed

- Nothing was removed in this release
