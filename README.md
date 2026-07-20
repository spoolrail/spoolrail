# Spoolrail

Spoolrail is a Laravel-native message broker package for publishing portable messages and handing consumed deliveries into Laravel Queue.

## Installation

```bash
composer require spoolrail/spoolrail
```

You can publish the config file with:

```bash
php artisan vendor:publish --tag=spoolrail-config
```

Resulting file: `config/spoolrail.php`

## Contributing

### Setup

```bash
git clone https://github.com/spoolrail/spoolrail.git
cd spoolrail
composer install
npm install
```

### Git Hooks

Install project hooks:

```bash
sh install-git-hooks.sh
```

Installed hooks:

- `pre-commit` runs `composer format`
- `pre-push` runs `composer analyse`

If you use Fork and hooks misbehave, see [this issue](https://github.com/fork-dev/Tracker/issues/996).

### Commands

| Command                  | Purpose                                            |
| ------------------------ | -------------------------------------------------- |
| `composer test`          | Run the test suite (`pest --compact`).             |
| `composer format`        | Run Laravel Pint and Prettier formatting.          |
| `composer analyse`       | Run static analysis (`phpstan`).                   |
| `composer refactor`      | Apply Rector refactors.                            |
| `composer coverage`      | Run tests with local coverage (`pest --coverage`). |
| `composer coverage:herd` | Run coverage via Laravel Herd tooling.             |

### Testing Lower Dependency Versions

To validate compatibility with Laravel 12 without editing `composer.json`:

```bash
composer update -W \
    illuminate/console:^12.0 \
    illuminate/contracts:^12.0 \
    illuminate/queue:^12.0 \
    illuminate/support:^12.0 \
    orchestra/testbench:^10.0 \
    pestphp/pest:^4.0 \
    pestphp/pest-plugin-laravel:^4.0
```

### Claude Setup (Optional)

`CLAUDE.md` is .gitignored by design. Expose `AGENTS.md` to Claude with a symlink or an import file.

### PhpStorm Setup (Optional)

Recommended setup for consistent formatting:

- `Settings | Editor | Code Style`: ensure "Enable EditorConfig support" is checked.
- `Settings | PHP | Quality Tools | Laravel Pint`: use ruleset from `pint.json`
- `Settings | PHP | Quality Tools`: set Laravel Pint as external formatter
- `Settings | Tools | Actions on Save`: enable reformat on save
- `Settings | Languages & Frameworks | JavaScript | Prettier`: use automatic config, enable "Run on save", and prefer Prettier config. Include `md` in Prettier file extensions.

### VSCode/Cursor Setup (Optional)

VSCode and Cursor will automatically detect formatting settings defined in the `.vscode/` folder – no additional setup is needed beyond installing the suggested extensions.

### Zed Setup (Optional)

This project does not maintain Zed editor configuration, but you may [download suggested config files from this Gist](https://gist.github.com/adiachenko/57feb8fb900453b33881e622e8152b67).
