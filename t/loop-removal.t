#!/usr/bin/env perl
use strict;
use warnings;

no indirect qw(fatal);
use utf8;

use Test::More;
use Test::NoLeaks;
use Future::AsyncAwait;
use Syntax::Keyword::Try;

use IO::Async::Loop;
use Database::Async;
use Database::Async::Engine::PostgreSQL;

my $loop = IO::Async::Loop->new;

my $uri = URI->new('postgresql://example@127.0.0.1:5000/empty?password=example-password');

# Attempt to connect and remove from the event loop a few times in succession.
# This would also need to confirm no FDs or other leftovers.
async sub cleanup_ok {
    test_noleaks(
        code => sub {
            (async sub {
                $loop->add(
                    my $instance = Database::Async->new(
                        uri => $uri
                    )
                );

                try {
                    await Future->wait_any(
                        $loop->timeout_future(after => 0.5),
                        $instance->query('select 1')->void,
                    );
                    await $loop->delay_future(after => 0.05);
                } catch ($e) {
                    note "failed - $e";
                    $instance->remove_from_parent;
                }
            })->()->get;
        },
        track_memory => 1,
        track_fds => 1,
        passes => 10,
        warmup_passes => 0,
        tolerate_hits => 1,
    );
}

subtest 'connection refused' => sub {
    # We set up a listener to grab a port, then shut it down
    # immediately afterwards. This gives us a port which is
    # "less" likely to be reässigned to anyone else in the short
    # time that this test runs.
    my $listener = $loop->listen(
        service => 0,
        socktype => 'stream',
        on_stream => sub {
            my ($stream) = @_;
            $stream->configure(on_read => sub { 0 });
            $loop->add($stream)
        }
    )->get;
    my $sock = $listener->read_handle;
    my $port = $sock->sockport;
    note "Listening on port ", $port, " - will close and try for connection-refused handling";
    $listener->configure(handle => undef);
    $listener->remove_from_parent;
    $sock->close;
    $uri->port($port);
    cleanup_ok()->get;
    done_testing;
};

subtest 'connection queued' => sub {
    # Here we want the incoming connections to sit in SYN_SENT state, waiting
    # in the TCP accept() queue backlog: we can override the acceptor in
    # IO::Async::Listener to achieve this.
    my $listener = $loop->listen(
        service => 0,
        socktype => 'stream',
        acceptor => sub {
            note 'accept() called, ignoring';
        },
        # No stream will ever succeed, so this can be empty
        on_stream => sub { },
    )->get;
    my $port = $listener->read_handle->sockport;
    note "Listening but not accepting on port ", $port;
    $uri->port($port);
    cleanup_ok()->get;
    done_testing;
};

subtest 'connection accepted but no response' => sub {
    my $listener = $loop->listen(
        service => 0,
        socktype => 'stream',
        on_stream => sub {
            my ($stream) = @_;
            # We don't want to read, and we don't want to write - just sit
            # there passively after accepting the connection
            $stream->configure(on_read => sub { 0 });
            $loop->add($stream)
        }
    )->get;
    my $port = $listener->read_handle->sockport;
    note "Listening on port ", $port;
    $uri->port($port);
    cleanup_ok()->get;
    done_testing;
};

