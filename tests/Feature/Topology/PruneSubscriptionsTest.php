<?php

declare(strict_types=1);

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Spoolrail\Spoolrail\Contracts\CanManageTopology;
use Spoolrail\Spoolrail\Exceptions\SubscriptionPruningException;
use Spoolrail\Spoolrail\Topology\PlanSubscriptionPruning;
use Spoolrail\Spoolrail\Topology\SubscriptionPruningPlan;
use Symfony\Component\Console\Output\BufferedOutput;

test('rejects blank option values', function (array $parameters, string $message): void {
    $exitCode = $this->artisan('spoolrail:prune-subscriptions', $parameters)
        ->expectsOutputToContain($message)
        ->run();

    expect($exitCode)->toBe(Command::FAILURE);
})->with([
    'connection' => [
        ['--connection' => ' '],
        'The --connection option must name a Spoolrail connection.',
    ],
    'retired prefix' => [
        ['--retired-prefix' => ' '],
        'The --retired-prefix option must name a former ownership prefix.',
    ],
]);

test('rejects a connection without package-managed topology', function (): void {
    expect(fn () => $this->artisan('spoolrail:prune-subscriptions', [
        '--connection' => 'array',
    ])->run())->toThrow(
        LogicException::class,
        'Spoolrail connection [array] does not provide package-managed topology.',
    );
});

test('refuses to prune the current prefix when the connection has no declared subscriptions', function (): void {
    // --- Arrange ---
    $planner = Mockery::mock(PlanSubscriptionPruning::class);
    $planner->expects('__invoke')
        ->once()
        ->with('managed', null)
        ->andThrow(SubscriptionPruningException::noDeclaredSubscriptions('managed'));
    app()->instance(PlanSubscriptionPruning::class, $planner);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:prune-subscriptions', [
        '--connection' => 'managed',
        '--force' => true,
    ])
        ->expectsOutputToContain(
            'Spoolrail connection [managed] has no declared subscriptions, so pruning its current ownership prefix was refused. Load the expected subscription declarations, or use [--retired-prefix] to delete a former prefix completely.',
        )
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(Command::FAILURE);
});

test('succeeds without confirmation when the pruning plan is empty', function (): void {
    // --- Arrange ---
    $topology = Mockery::mock(CanManageTopology::class);
    $topology->shouldNotReceive('deleteSubscription');

    $planner = Mockery::mock(PlanSubscriptionPruning::class);
    $planner->expects('__invoke')
        ->once()
        ->with('managed', null)
        ->andReturn(new SubscriptionPruningPlan($topology, 'current-app', []));
    app()->instance(PlanSubscriptionPruning::class, $planner);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:prune-subscriptions', [
        '--connection' => 'managed',
    ])
        ->expectsOutputToContain('No undeclared subscription resources were found.')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(Command::SUCCESS);
});

test('shows the pruning plan and leaves it untouched when confirmation is declined', function (): void {
    // --- Arrange ---
    $topology = Mockery::mock(CanManageTopology::class);
    $topology->shouldNotReceive('deleteSubscription');

    $planner = Mockery::mock(PlanSubscriptionPruning::class);
    $planner->expects('__invoke')
        ->once()
        ->with('managed', null)
        ->andReturn(new SubscriptionPruningPlan(
            $topology,
            'planned-prefix',
            ['current-old-orders', 'current-old-invoices'],
        ));
    app()->instance(PlanSubscriptionPruning::class, $planner);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:prune-subscriptions', [
        '--connection' => 'managed',
    ])
        ->expectsOutputToContain(
            'Inspecting connection [managed] with ownership prefix [planned-prefix].',
        )
        ->expectsOutputToContain(
            'Pruning will permanently delete 2 subscription resources and discard any messages waiting for delivery.',
        )
        ->expectsOutputToContain('[current-old-orders]')
        ->expectsConfirmation('Delete the displayed subscription resources?', 'no')
        ->expectsOutputToContain('Subscription pruning cancelled. Use [--force] to skip confirmation.')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(Command::FAILURE);
});

test('deletes the displayed pruning plan after confirmation', function (): void {
    // --- Arrange ---
    $topology = Mockery::mock(CanManageTopology::class);
    $topology->expects('deleteSubscription')->once()->with('current-old-orders');

    $planner = Mockery::mock(PlanSubscriptionPruning::class);
    $planner->expects('__invoke')
        ->once()
        ->with('managed', null)
        ->andReturn(new SubscriptionPruningPlan(
            $topology,
            'current-app',
            ['current-old-orders'],
        ));
    app()->instance(PlanSubscriptionPruning::class, $planner);

    // --- Act ---
    $exitCode = $this->artisan('spoolrail:prune-subscriptions', [
        '--connection' => 'managed',
    ])
        ->expectsConfirmation('Delete the displayed subscription resources?', 'yes')
        ->expectsOutputToContain('Deleted subscription resource [current-old-orders].')
        ->run();

    // --- Assert ---
    expect($exitCode)->toBe(Command::SUCCESS);
});

test('deletes the displayed pruning plan without confirmation when forced', function (): void {
    // --- Arrange ---
    $topology = Mockery::mock(CanManageTopology::class);
    $topology->expects('deleteSubscription')->once()->with('current-old-orders');
    $topology->expects('deleteSubscription')->once()->with('current-old-invoices');

    $planner = Mockery::mock(PlanSubscriptionPruning::class);
    $planner->expects('__invoke')
        ->once()
        ->with('managed', null)
        ->andReturn(new SubscriptionPruningPlan(
            $topology,
            'current-app',
            ['current-old-orders', 'current-old-invoices'],
        ));
    app()->instance(PlanSubscriptionPruning::class, $planner);
    $output = new BufferedOutput;

    // --- Act ---
    $exitCode = Artisan::call('spoolrail:prune-subscriptions', [
        '--connection' => 'managed',
        '--force' => true,
    ], $output);

    // --- Assert ---
    expect($exitCode)->toBe(Command::SUCCESS);
    expect($output->fetch())
        ->toContain('Pruning will permanently delete 2 subscription resources and discard any messages waiting for delivery.')
        ->toContain('[current-old-orders]')
        ->toContain('[current-old-invoices]')
        ->toContain('Deleted subscription resource [current-old-orders].')
        ->toContain('Deleted subscription resource [current-old-invoices].');
});
