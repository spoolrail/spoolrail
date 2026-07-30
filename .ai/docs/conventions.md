# Package Conventions

## General

- Favor Laravel collection methods when they are at least as clear as the equivalent loop.
- MUST NOT use `final`.
- Add type declarations to package-owned code wherever compatible with PHP and framework contracts and conventions.
- Use docblocks only when they add information native PHP declarations cannot express.
- Inline single-use locals when the resulting expression remains clear.
- Prefer string interpolation over `sprintf()` or `.` concatenation. Use concatenation for multi-line or complex strings when it improves readability.
- Use `$var` instead of `{$var}` in interpolated strings unless required for disambiguation.

## Configuration

- In multi-section configuration files, use blank lines inside the outer array and between top-level groups or multi-line sibling configurations.
- Use Laravel-style section comments when a top-level group needs a heading or explanation; document only behavior or constraints not evident from its keys and defaults.
- Use inline `env()` calls for common deployment-specific values, providing defaults when safe and meaningful. Leave uncommon opt-in values at literal defaults so applications choose how to source them.
- Access configuration where it is interpreted, with explicit defaults; avoid wrappers that only relay values. For complex configuration blocks, follow how analogous components in the Laravel framework and its first-party packages read and interpret their configuration.
- Treat configuration as trusted application input. Do not add defensive validation merely to fail earlier or produce friendlier errors.

## Exceptions: Centralize, Don't Scatter

- Let exceptions bubble to the host application or framework boundary by default.
- Add `try/catch` only when it meaningfully changes behavior through recovery or rollback, retry, fallback, or exception translation. Never use it for routine control flow.
- Use `finally` only for guaranteed resource cleanup (locks, temp files, external handles).
- Design exception contracts around the actions callers can take. Related failures may share a type only when callers can handle them the same way or distinguish them through structured state; messages are diagnostic, not control flow.
- Translate lower-level exceptions only at a boundary that owns a more stable failure contract, preserving the original exception as the cause.
- Let exceptions from caller-supplied callbacks and delegated application work propagate unchanged unless the current boundary owns their recovery semantics.
- Choose an exception's base class according to the catch boundary callers need, preferring the closest suitable SPL or framework exception class.
- Domain exception classes own their meaningful messages; use named constructors when they clarify distinct failure cases.

### Packages

- Package-defined exceptions also implement a shared marker interface, providing a package-wide catch boundary independently of their base class.

## Controllers

Invokable `VerbNounController` (e.g. `StorePostController`). Avoid noun-first names like `PostController`.

## Naming Conventions

Omit Abstract/Interface/Contract/Trait from class names.

Treat acronyms as words (`HttpClient`, not `HTTPClient`). Exception: two-letter acronyms (`ID`, `UI`).

Name package-owned booleans as clear predicates, typically with `is`, `has`, `can`, or `should`.

- **Actions**: `VerbNounAction`, e.g. `CreateOrderAction`, `SendInvitationAction`.
- **Commands**: Mirror the Artisan signature, kebab-case multi-word, e.g. `package:flush-records` → `FlushRecordsCommand`.
- **Data**: `NounData`, e.g. `OrderData`, `UserProfileData`.
- **Enums**: No prefix/suffix, e.g. `OrderStatus`, `SubscriptionType`.
- **Events**: Tense conveys timing — progressive before (`RequestSending`), past after (`Registered`).
- **Facades**: Singular nouns, no suffix, e.g. `Inventory`, `Geocoder`.
- **Jobs**: Action + `Job` suffix, e.g. `CreateUserJob`, `PerformDatabaseCleanupJob`.
- **Listeners**: Action + `Listener` suffix, e.g. `SendInvitationMailListener`.
- **Mailables**: Noun + `Mail` suffix, e.g. `OrderConfirmationMail`.
- **Notifications**: Past tense + `Notification` suffix, e.g. `EmployeeAccountCreatedNotification`.
- **Services**: External service name + `Service`, e.g. `StripeService`.
