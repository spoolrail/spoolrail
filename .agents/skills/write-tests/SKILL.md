---
name: write-tests
description: Use when creating, editing, fixing, organizing, reviewing, or running tests in this Laravel package. Covers package-specific suite placement, test support, naming, structure, assertions, external-test boundaries, and execution scope.
---

# Write Tests

Apply this package-specific testing policy alongside any applicable general test-design or framework-specific testing skills.

## Approach

- Use Pest with Orchestra Testbench, and keep the test namespace aligned with Composer `autoload-dev`.
- Start from the package's normally loaded configuration and Testbench defaults. Put suite-wide environment defaults in `phpunit.xml.dist`; keep per-test overrides to values material to the scenario instead of routinely reconstructing configuration registries.

## Suite Placement

- `tests/Feature`: default. Test package behavior from the documented public API inward, asserting only externally observable outcomes. Group files by public capability rather than production namespace, and split a capability only when a supported variant requires distinct scenarios. Test internal machinery in `Unit`.
- `tests/Unit`: tests for individual classes, mirroring `src/` namespaces. Strict isolation is not required.
- `tests/External`: tests against real services that are not part of the repository-managed ordinary test environment, organized by provider—for example, credentialed remote APIs.
- `tests/ArchTest.php`: package-wide architectural constraints. Protect durable design boundaries without encoding incidental implementation structure.

## Test Support

- Place fixture classes and files in `tests/Fixtures`.
- Place reusable infrastructure setup and inspection helpers in `tests/Concerns`.

## File Naming

Always use the `Test.php` suffix.

- Feature tests group scenarios for the same action in one `{Verb}{Noun}Test.php` file, such as `CreateOrderTest.php` or `SendInvitationTest.php`. The action owns the file name regardless of which internal classes implement it.
- Unit tests mirror the class, such as `UserTest.php` or `CreateUserJobTest.php`.
- External tests use descriptive names reflecting what's tested, such as `StripeWebhookTest.php`.

## Test Descriptions

Use `<present-tense verb> <observable outcome> [when <condition>]`.

```php
test('returns validation errors when required registration fields are missing', function () {});
```

## Test Structure

Structure tests with meaningful setup, action, and verification work in explicit Arrange, Act, and Assert phases, using the matching `// --- Arrange ---`, `// --- Act ---`, and `// --- Assert ---` banner comments. Keep tests without three meaningful phases direct.

## Assertions

Prefer explicit assertions that keep each subject and expectation easy to identify. Chain only closely related expectations when doing so makes the test easier to read.

## Execution

Run the smallest test scope that proves the change, then expand when risk increases.

Test against the currently installed dependencies by default. Run compatibility tests across multiple framework versions only at major project milestones or when explicitly requested.

```bash
composer test -- --filter='registers the service provider'
composer test -- tests/Unit
composer test -- --testsuite=Unit
```

Reserve the full mutation suite for major project milestones and explicit requests, after the ordinary test suite passes.

External tests use billable production services. Run them only at the user's request or after asking and receiving permission, and announce the run before execution. Exclude them from routine verification, target only the relevant provider during setup or diagnosis, and follow `tests/External/README.md` for setup and commands.
