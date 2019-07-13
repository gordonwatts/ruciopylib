# ruciopylib
 Simple python library that uses `os.system` to run rucio and download datasets to a local directory. Includes tools to help manage certificates.

## Usage

The most interesting high level classes here are:

- `cert` Used to keep a GRID certificate authorized.
- `rucio_cache_interface` used to get catalogs of existings `rucio` datasets and download the files locally.

## Development Work

 This package uses `pytest` for tests.