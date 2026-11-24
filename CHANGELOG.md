# Changelog

## [Unreleased]

## [0.6.0] - 2026-11-24

### Added

- The sink declares `NestedTypes`: list, struct and map columns (and fixed-size lists, written as Delta
  lists) are written with nulls at every level and read back by delta-rs and DuckDB.
- A nested column used as a merge key or a partition column is refused by name ("a merge key must be a
  flat value", "a partition column must be a flat value").
