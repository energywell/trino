# Product Test Audit Gap Summary

No unresolved legacy-to-current fidelity gaps remain on this branch.

All previously recorded suite-breadth, CLI, Hive, Iceberg, and compatibility gaps were addressed in code and
revalidated against the audit docs. The items still tracked below are intentional retirements or non-gap reviewer notes,
not open parity failures.

Intentional retired legacy methods:

- `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestSqlCancel.java` ->
  `testCancelledQueryShowsCancelInfo`
- `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestSqlCancel.java` ->
  `testKilledQueryShowsKillInfo`
- `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestSqlCancel.java` ->
  `testCancelledTaskShowsTaskCancelInfo`
  - These were intentionally retired as unscheduled legacy-only coverage.
  - They are tracked separately from the unresolved mapping gaps above.

The current audit state also reflects these completed structural checks:

- Every concrete current `@ProductTest` class is now represented in the lane audits.
- Every documented `Current target method` entry resolves to a real current file/method in the tree.

Current source/history plus suite-selection revalidation did surface recorded non-gap differences that still matter to
reviewers:

- Current-only JUnit environment verification classes are explicitly documented in the lane audits. Some are unscheduled
  direct/IDE verification coverage, while others are routed by current suites such as `SuiteIceberg`.
- `TestHiveTableStatistics` is not selected by any current suite. The legacy launcher `SuiteHive4` also did not select
  it because it filtered on `HIVE4` and `CONFIGURED_FEATURES` while the class carried neither tag.
- `TestHiveTransactionalEnvironment.testVerifyEnvironmentHiveTransactionalByDefault` carries a legacy assertion moved
  from `TestHiveCreateTable`, but the surrounding environment verification class is currently unscheduled and is now
  documented as current-only helper coverage rather than implied CI coverage.
