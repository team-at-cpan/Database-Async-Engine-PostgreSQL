use strict;
use warnings;

use feature qw(state);
no indirect;

use Test::More;
use Test::Fatal;
use Test::Deep;
use Future::AsyncAwait;
use IO::Async::Loop;
use Database::Async;
use Database::Async::Engine::PostgreSQL;
use Log::Any::Adapter qw(TAP);
use Log::Any qw($log);

plan skip_all => 'set DATABASE_ASYNC_PG_TEST env var to test, but be prepared for it to *delete any and all data* in that database' unless exists $ENV{DATABASE_ASYNC_PG_TEST};

my $loop = IO::Async::Loop->new;

my $db;
is(exception {
    $loop->add(
        $db = Database::Async->new(
            type => 'postgresql',
        )
    );
}, undef, 'can safely add to the loop');

$log->infof('Sending a valid query');
await $db->query('SELECT 1;')->single;
$log->infof('Basic query works');
my $exception = exception{$db->query('SELECT 1/0')->void->get};
isa_ok ($exception, 'Protocol::Database::PostgreSQL::Error', 'Query failed as expected');
$log->infof('Sending a new query through the same connection should works');
await $db->query('SELECT 1;')->single;

done_testing;

