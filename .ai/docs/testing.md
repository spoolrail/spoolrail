# Testing Guidelines

## Approach

- Use Pest + Orchestra Testbench, and keep the test namespace aligned with Composer `autoload-dev`.
- Start tests from the package's normally loaded configuration and Testbench defaults. Put suite-wide environment defaults in `phpunit.xml.dist`; keep per-test overrides to values material to the scenario instead of reconstructing configuration registries as routine setup.

## Test Suites

- `tests/Feature`: default. Test package behavior from the documented public API inward, asserting only externally observable outcomes. Group files by public capability rather than production namespace, and split a capability only when a supported variant requires distinct scenarios. Test internal machinery in `Unit`.
- `tests/Unit`: tests for individual classes, mirroring `src/` namespaces. Strict isolation NOT required.
- `tests/External`: tests against real services that are not part of the repository-managed ordinary test environment, organized by provider—for example, credentialed remote APIs.

## Test Support

- Place fixture classes and files in `tests/Fixtures`.
- Place reusable infrastructure setup and inspection helpers in `tests/Concerns`.

## File Naming

Always use `Test.php` suffix.

- **Feature**: group scenarios for the same action in one `{Verb}{Noun}Test.php` file (e.g. `CreateOrderTest.php`, `SendInvitationTest.php`). The action owns the file name regardless of which internal classes implement it.
- **Unit**: mirror the class — `{ClassName}Test.php` (e.g. `UserTest.php`, `CreateUserJobTest.php`).
- **External**: descriptive names reflecting what's tested (e.g. `StripeWebhookTest.php`).

## Test Descriptions

Pattern: `<present-tense verb> <observable outcome> [when <condition>]`

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
composer test -- --filter='can test'
composer test -- tests/Unit/PackageNameServiceProviderTest.php
composer test -- --testsuite=Unit
```

## Verification

After changes, run `composer format` and `composer analyse` before finalizing.
