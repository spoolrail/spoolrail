# Testing Guidelines

## Approach

- Use Pest + Orchestra Testbench, and keep the test namespace aligned with Composer `autoload-dev`.
- For behavior changes and bug fixes, prefer starting with a failing test that captures the expected outcome, then implement the change.
- Start tests from the package's normally loaded configuration and Testbench defaults. Put suite-wide environment values in `phpunit.xml.dist`; keep per-test overrides to values material to the scenario instead of reconstructing configuration registries as routine setup.
- Avoid mocks unless the dependency crosses an external I/O boundary, represents a supported extension point, has behavior that is already proved more directly elsewhere, or would make the test impractical to run locally. Prefer real application objects.

## Test Suites

- `tests/Feature`: default. Test package behavior from the documented public API inward, asserting only externally observable outcomes. Test internal machinery in `Unit`.
- `tests/Unit`: tests for individual classes, mirroring `src/` namespaces. Strict isolation NOT required.
- `tests/External`: real interactions with external services (no mocking), organized by provider.

Unmocked external calls belong in `tests/External` only. Drivers that don't hit external services belong in `Unit`; drivers that do belong in `External`.

## Fixtures

Place fixture classes and files in `tests/Fixtures`.

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

Prefer explicit, single-purpose assertions over dense chained helpers.

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
