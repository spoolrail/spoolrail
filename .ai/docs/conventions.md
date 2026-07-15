# Package Conventions

## General

- Prefer guard clauses and straight-line control flow; keep nesting shallow.
- Avoid boolean flags — prefer distinct methods, named option objects, or separate functions.
- Favor Laravel collection methods when they are at least as clear as the equivalent loop.
- MUST NOT use `final`.
- All class properties, method parameters, and return types MUST have type declarations.
- Use docblocks only to expand on complex types (arrays, collections) or when a description adds context beyond the signature.
- Prefer string interpolation over `sprintf()` or `.` concatenation. Use concatenation for multi-line or complex strings when it improves readability.
- Use `$var` instead of `{$var}` in interpolated strings unless required for disambiguation.
- Read `config()` inline at call sites with explicit defaults — no pass-through wrappers.

## Method Complexity

Methods should operate at a single level of abstraction — describe _what_ happens, not _how_. If a method exceeds ~20 lines of logic (excluding validation arrays and return statements), it likely mixes concerns.

**Signs a method needs extraction:**

- Nested closures or callbacks with their own branching
- Multiple try/catch blocks or catch-and-retry patterns
- Data formatting or transformation mixed with business logic
- Sequential steps that each deserve a descriptive name

## Exceptions: Centralize, Don't Scatter

- Let exceptions bubble to the host application or framework boundary by default.
- Add `try/catch` only when it meaningfully changes behavior: a fallback path, retry, or converting to a domain exception. Never use it for routine control flow.
- Use `finally` only for guaranteed resource cleanup (locks, temp files, external handles).

## Controllers

Invokable `VerbNounController` (e.g. `StorePostController`). Avoid noun-first names like `PostController`.

## Naming Conventions

Omit Abstract/Interface/Contract/Trait from class names.

Treat acronyms as words (`HttpClient`, not `HTTPClient`). Exception: two-letter acronyms (`ID`, `UI`).

Booleans: prefix with `is`, `has`, `can`, `should`.

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
