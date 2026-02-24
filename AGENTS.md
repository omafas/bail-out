# NS Server - Agent Guidelines

## Project Overview

ns_server is the cluster manager for Couchbase Server, written primarily in Erlang. It uses rebar3 for building and CMake as a wrapper for integration with the broader Couchbase build system.

## Running Tests

### Detect Build System
Before building anything - two build systems are used by Couchbase Server, make and ninja. If ninja is being used, a `build.ninja` file will be present in the `build` directory:
```bash
ls ../build/build.ninja 2>/dev/null && echo "ninja" || echo "make"
```

### Dialyzer Static Analysis

Run Dialyzer to perform static analysis and detect type discrepancies. Only relevant for Erlang changes:
```bash
make dialyzer
ninja -C ../build ns_dialyzer
```

### EUnit Tests

Run all eunit tests:
```bash
make test_eunit
ninja -C ../build ns_test
```

Run tests for a specific module:
```bash
T_WILDCARD=<module_name> make test_eunit
T_WILDCARD=<module_name> ninja -C ../build ns_test
```

Example:
```bash
T_WILDCARD=menelaus_web_rbac make test_eunit
T_WILDCARD=menelaus_web_rbac ninja -C ../build ns_test
```

### Other Test Targets

- `make test` - runs all tests
- `make test_triq` - runs property-based tests (triq)
- `make cbcollect_tests` - runs cbcollect tests

### Cluster Tests

Python-based integration tests that run against a real cluster.
The test framework starts the cluster automatically.

Run specific test:
```bash
./run.py --tests <TESTSET_NAME>.<TEST_NAME>
```

Run specific test set:
```bash
./run.py --tests TESTSET_NAME
```

**CRITICAL:** `<TESTSET_NAME>` must be the **Python class name** (e.g., `AlertTests`, `BasicBucketTestSet`), NOT the filename. Test set classes are defined in `.py` files in `ns_server/cluster_tests/testsets`.

Run all cluster tests (may take several hours, don't run unless explicitly asked):
```bash
cd cluster_tests
./run.py
```

Available test sets include: `BasicBucketTestSet`, `MultiNodeBucketTestSet`, `CrudTests`, `CollectionTests`, `UsersTestSet`, `AuthnTests`, `StatsTests`, and many more. Run `./run.py --list` to see the full list.

## Project Structure

- `apps/` - Erlang applications
  - `apps/ns_server/` - main ns_server application
  - `apps/ale/` - logging framework
  - `apps/ns_babysitter/` - process supervision
  - `apps/ns_common/` - common utilities
  - `apps/ns_couchdb/` - CouchDB integration
  - `apps/config_remap/` - configuration remapping
- `deps/` - external dependencies
- `scripts/` - utility scripts
- `priv/` - private data files
- `cluster_tests/` - cluster-level integration tests

## Coding Conventions

- Follow existing Erlang conventions in the codebase
- Tests are located alongside source files or in `test/` directories within each app
- Include `-include_lib("eunit/include/eunit.hrl").` for modules with eunit tests
- For all Erlang, Python, and Go files, max line length is strictly 80 characters
- Trailing spaces and tabs are not allowed in any Erlang, Python, or Go files
