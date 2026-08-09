# Contributing

## Setup

```bash
git clone https://github.com/spoolrail/spoolrail.git
cd spoolrail
composer install
npm install
```

## RabbitMQ

RabbitMQ 4.3 is an internal development and testing dependency. Start the repository-managed broker and its Management HTTP API before running the test suite:

```bash
docker compose up -d --wait rabbitmq
composer test
```

The default endpoints are `amqp://spoolrail:spoolrail@127.0.0.1:5672/spoolrail` and `http://127.0.0.1:15672`. The test defaults live in `phpunit.xml.dist`. If either host port is occupied, set `SPOOLRAIL_RABBITMQ_PORT` or `SPOOLRAIL_RABBITMQ_MANAGEMENT_PORT` before starting Compose and supply the matching `RABBITMQ_PORT` or `RABBITMQ_MANAGEMENT_URL` when running tests.

RabbitMQ scenarios are part of the ordinary Feature suite. `composer test` never starts, restarts, or stops Docker containers; it fails clearly rather than silently skipping those scenarios when the broker is unavailable. Stop and remove the service when it is no longer needed:

```bash
docker compose down --volumes
```

## Git Hooks

Install project hooks:

```bash
sh install-git-hooks.sh
```

Installed hooks:

- `pre-commit` runs `composer format:check`
- `pre-push` runs `composer analyse`

If you use Fork and hooks misbehave, see [this issue](https://github.com/fork-dev/Tracker/issues/996).

## Commands

> Mutation testing and coverage commands require Xdebug in coverage mode or enabled PCOV.

| Command                  | Purpose                                             |
| ------------------------ | --------------------------------------------------- |
| `composer test`          | Run the default suite, including RabbitMQ coverage. |
| `composer format`        | Run Laravel Pint and Prettier formatting.           |
| `composer format:check`  | Check Laravel Pint and Prettier formatting.         |
| `composer analyse`       | Run static analysis (`phpstan`).                    |
| `composer refactor`      | Apply Rector refactors.                             |
| `composer mutate`        | Run mutation testing across the package.            |
| `composer mutate:herd`   | Run mutation testing via Laravel Herd tooling.      |
| `composer coverage`      | Run tests with local coverage (`pest --coverage`).  |
| `composer coverage:herd` | Run coverage via Laravel Herd tooling.              |

## Testing Lower Dependency Versions

To validate compatibility with Laravel 12 without editing `composer.json`:

```bash
composer update -W \
    illuminate/console:^12.0 \
    illuminate/contracts:^12.0 \
    illuminate/http:^12.0 \
    illuminate/queue:^12.0 \
    illuminate/support:^12.0 \
    orchestra/testbench:^10.0 \
    pestphp/pest:^4.0 \
    pestphp/pest-plugin-laravel:^4.0
```

The Pest 5 upgrade is deferred until after Laravel 12 support is dropped due to [pestphp/pest#1772](https://github.com/pestphp/pest/pull/1772).

## Claude Setup (Optional)

Claude-specific files are ignored so the repository can keep `AGENTS.md` and `.agents/skills` as its canonical agent guidance. Expose them to Claude Code with local symlinks:

```bash
ln -s AGENTS.md CLAUDE.md

mkdir -p .claude
ln -s ../.agents/skills .claude/skills
```

## Editor Setup (Optional)

### Zed

Zed automatically loads the shared settings from `.zed/settings.json`, whether this repository is opened directly or through its parent workspace.

Please install the following extensions:

- https://zed.dev/extensions/editorconfig
- https://zed.dev/extensions/laravel-official

### VSCode/Cursor

When the project is opened directly, VSCode and Cursor automatically load the shared configuration from `.vscode/`. Please install the automatically suggested extensions.

However, unlike Zed, they do not discover this configuration when the project is loaded as part of a larger monorepo; that workspace must provide its own editor configuration.

### PhpStorm

Recommended setup for consistent formatting:

- `Settings | Editor | Code Style`: ensure "Enable EditorConfig support" is checked.
- `Settings | PHP | Quality Tools | Laravel Pint`: set "Path to pint.json" to `pint.json` and select "defined in pint.json" as the ruleset
- `Settings | PHP | Quality Tools`: set Laravel Pint as external formatter
- `Settings | Tools | Actions on Save`: enable reformat on save
- `Settings | Languages & Frameworks | JavaScript | Prettier`: use automatic config, enable "Run on save", and prefer Prettier config. Include `md` in Prettier file extensions.

When opened through a parent monorepo, point Laravel Pint explicitly to this repository's `vendor/bin/pint` and `pint.json`. However, PhpStorm will have to apply these settings project-wide. Sibling PHP projects won't be able to use different Pint rules.
