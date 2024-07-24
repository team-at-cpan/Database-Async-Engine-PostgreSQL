#!/usr/bin/env perl 
use strict;
use warnings;
use Test::More;
use Syntax::Keyword::Try qw(try :experimental(typed));
use Object::Pad;
use Future::AsyncAwait;
use Ryu::Async;
use IO::Async::Loop;
use Database::Async;
use Database::Async::Engine::PostgreSQL;
use Log::Any::Adapter qw(TAP);
use Log::Any qw($log);

die 'set DATABASE_ASYNC_PG_SERVICE env var to test, but be prepared for it to *delete any and all data* in that database' unless exists $ENV{DATABASE_ASYNC_PG_SERVICE};

my $loop = IO::Async::Loop->new;

my $app = 'benchmark';
$loop->add(
    my $db = Database::Async->new(
        type => 'postgresql',
        pool => {
            max => 1,
        },
        engine => {
            service          => $ENV{DATABASE_ASYNC_PG_SERVICE},
            application_name => $app,
        },
    )
);
$loop->add(
    my $ryu = Ryu::Async->new
);

for my $iteration (1..5) {
    {
        my ($v) = await $db->query('select 1 as "value"')->row_hashrefs->as_list;
        is($v->{value}, 1, 'have value from successful query');
    }
    try {
        my ($v) = await $db->query('select 1/0 as "value"')->row_hashrefs->as_list;
        is($v->{value}, 1, 'exception thrown from PostgreSQL');
    } catch ($e isa Protocol::Database::PostgreSQL::Error) {
        is($e->code, 22012, 'have expected error code from PG');
        is($e->severity, 'ERROR', 'marked as error');
        like($e->message, qr/division by zero/, 'message matches our expectations too');
    }
    {
        my ($v) = await $db->query('select 1 as "value"')->row_hashrefs->as_list;
        is($v->{value}, 1, 'have value from successful query after error');
    }
}
done_testing;
