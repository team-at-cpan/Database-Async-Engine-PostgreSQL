package Database::Async::Engine::PostgreSQL;
# ABSTRACT: PostgreSQL support for Database::Async

use strict;
use warnings;

use utf8;

our $VERSION = '1.004';

use parent qw(Database::Async::Engine);

=encoding utf8

=head1 NAME

Database::Async::Engine::PostgreSQL - support for PostgreSQL databases in L<Database::Async>

=head1 DESCRIPTION

Provide a C<postgresql://> URI when instantiating L<Database::Async> to use this engine.

 $loop->add(
  my $dbh = Database::Async->new(
   uri => 'postgresql://localhost'
  )
 );

Connection can also be made using a service definition, as described in L<https://www.postgresql.org/docs/current/libpq-pgservice.html>.

 $loop->add(
  my $dbh = Database::Async->new(
   type => 'postgresql',
   engine => {
    service => 'example',
   }
  )
 );

If neither URI nor service are provided, the C<PGSERVICE> environment variable is attempted, and will fall back
to localhost (similar to C<psql -h localhost> behaviour).

 $loop->add(
  my $dbh = Database::Async->new(
   type => 'postgresql',
  )
 );


=cut

no indirect;
use Syntax::Keyword::Try;
use Ryu::Async;
use Ryu::Observable;
use curry;
use Scalar::Util ();
use URI::postgres;
use URI::QueryParam;
use Future::AsyncAwait;
use Database::Async::Query;
use File::HomeDir;
use Config::Tiny;
use Encode ();
use MIME::Base64 ();
use Bytes::Random::Secure ();
use Unicode::UTF8;
use Crypt::Digest::SHA256 ();
use Crypt::Mac::HMAC ();

use Protocol::Database::PostgreSQL::Client 2.000;
use Protocol::Database::PostgreSQL::Constants qw(:v1);

use Log::Any qw($log);

Database::Async::Engine->register_class(
    postgresql => __PACKAGE__
);

=head1 METHODS

=head2 configure

=cut

sub configure {
    my ($self, %args) = @_;
    for (qw(service encoding application_name)) {
        $self->{$_} = delete $args{$_} if exists $args{$_};
    }
    return $self->next::method(%args);
}

sub encoding { shift->{encoding} }

=head2 connection

Returns a L<Future> representing the database connection,
and will attempt to connect if we are not already connected.

=cut

sub connection {
    my ($self) = @_;
    $self->{connection} //= $self->connect;
}

=head2 ssl

Whether to try SSL or not, expected to be one of the following
values from L<Protocol::Database::PostgreSQL::Constants>:

=over 4

=item * C<SSL_REQUIRE>

=item * C<SSL_PREFER>

=item * C<SSL_DISABLE>

=back

=cut

sub ssl { shift->{ssl} }

=head2 read_len

Buffer read length. Higher values mean we will attempt to read more
data for each I/O loop iteration.

Defaults to 2 megabytes.

=cut

sub read_len { shift->{read_len} //= 2 * 1024 * 1024 }

=head2 write_len

Buffer write length. Higher values mean we will attempt to write more
data for each I/O loop iteration.

Defaults to 2 megabytes.

=cut

sub write_len { shift->{write_len} //= 2 * 1024 * 1024 }

=head2 connect

Establish a connection to the server.

Returns a L<Future> which resolves to the L<IO::Async::Stream>
once ready.

=cut

async sub connect {
    my ($self) = @_;
    my $loop = $self->loop;

    my $connected = $self->connected;
    die 'We think we are already connected, and that is bad' if $connected->as_numeric;

    # Initial connection is made directly through the URI
    # parameters. Eventually we also want to support UNIX
    # socket and other types.
    $self->{uri} ||= $self->uri_for_service($self->service) if $self->service;
    my $uri = $self->uri;
    die 'bad URI' unless ref $uri;
    $log->tracef('URI for connection is %s', "$uri");
    my $endpoint = join ':', $uri->host, $uri->port;

    $log->tracef('Will connect to %s', $endpoint);
    $self->{ssl} = do {
        my $mode = $uri->query_param('sslmode') // 'prefer';
        $Protocol::Database::PostgreSQL::Constants::SSL_NAME_MAP{$mode} // die 'unknown SSL mode ' . $mode;
    };

    # We're assuming TCP (either v4 or v6) here, but there's not really any reason we couldn't have
    # UNIX sockets or other transport layers here other than lack of demand so far.
    my @connect_params;
    if ($uri->host and not $uri->host =~ m!^[/@]!) {
        @connect_params = (
            service     => $uri->port,
            host        => $uri->host,
            socktype    => 'stream',
        );
    } elsif ($uri->host eq '') {
        @connect_params = (
            addr => {
                family   => 'unix',
                socktype => 'stream',
                path     => '/var/run/postgresql/.s.PGSQL.'.$uri->port,
            }
        );
    } else {
        @connect_params = (
            addr => {
                family   => 'unix',
                socktype => 'stream',
                path     => $uri->host.'/.s.PGSQL.'.$uri->port,
            }
        );
    }
    my $sock = await $loop->connect(@connect_params);

    if ($sock->sockdomain == Socket::PF_INET or $sock->sockdomain == Socket::PF_INET6) {
        my $local  = join ':', $sock->sockhost_service(1);
        my $remote = join ':', $sock->peerhost_service(1);
        $log->tracef('Connected to %s as %s from %s', $endpoint, $remote, $local);
    } elsif ($sock->sockdomain == Socket::PF_UNIX) {
        $log->tracef('Connected to %s as %s', $endpoint, $sock->peerpath);
    }

    # We start with a null handler for read, because our behaviour varies depending on
    # whether we want to go through the SSL dance or not.
    $self->add_child(
        my $stream = IO::Async::Stream->new(
            handle   => $sock,
            on_read  => sub { 0 }
        )
    );

    # SSL is conveniently simple: a prefix exchange before the real session starts,
    # and the user can decide whether SSL is mandatory or optional.
    $stream = await $self->negotiate_ssl(
        stream => $stream,
    );

    Scalar::Util::weaken($self->{stream} = $stream);
    $self->outgoing->each(sub {
        $log->tracef('Write bytes [%v02x]', $_);
        $self->ready_for_query->set_string('');
        $self->stream->write("$_");
        return;
    });
    $stream->configure(
        on_read   => $self->curry::weak::on_read,
        read_len  => $self->read_len,
        write_len => $self->write_len,
        autoflush => 0,
    );

    $log->tracef('Send initial request with user %s', $uri->user);

    # This is where the extensible options for initial connection are applied:
    # we have already handled SSL by this point, so we exclude this from the
    # list and pass everything else directly to the startup packet.
    my %qp = $uri->query_params;
    delete $qp{sslmode};

    $qp{application_name} //= $self->application_name;
    $self->protocol->send_startup_request(
        database         => $self->database_name,
        user             => $self->database_user,
        %qp
    );
    $connected->set_numeric(1);
    return $stream;
}

=head2 service_conf_path

Return the expected location for the pg_service.conf file.

=cut

sub service_conf_path {
    my ($class) = @_;
    return $ENV{PGSERVICEFILE} if exists $ENV{PGSERVICEFILE};
    return $ENV{PGSYSCONFDIR} . '/pg_service.conf' if exists $ENV{PGSYSCONFDIR};
    my $path = File::HomeDir->my_home . '/.pg_service.conf';
    return $path if -r $path;
    return '/etc/pg_service.conf';
}

sub service_parse {
    my ($class, $path) = @_;
    return Config::Tiny->read($path, 'encoding(UTF-8)');
}

sub find_service {
    my ($class, $srv) = @_;
    my $data = $class->service_parse(
        $class->service_conf_path
    );
    die 'service ' . $srv . ' not found in config' unless $data->{$srv};
    return $data->{$srv};
}

sub service { shift->{service} //= $ENV{PGSERVICE} }

sub database_name {
    my $uri = shift->uri;
    return $uri->dbname // $uri->user // 'postgres'
}

sub database_user {
    my $uri = shift->uri;
    return $uri->user // 'postgres'
}

sub password_from_file {
    my $self = shift;
    my $pwfile = $ENV{PGPASSFILE} || File::HomeDir->my_home . '/.pgpass';

    unless ($^O eq 'MSWin32') { # same as libpq
        # libpq also does stat here instead of lstat. So, pgpass can be
        # a logical link.
        my (undef, undef, $mode) = stat $pwfile or return undef;
        unless (-f _) {
            $log->warnf("WARNING: password file \"%s\" is not a plain file\n", $pwfile);
            return undef;
        }

        if ($mode & 077) {
            $log->warnf("WARNING: password file \"%s\" has group or world access; permissions should be u=rw (0600) or less", $pwfile);
            return undef;
        }
        # libpq has the same race condition of stat versus open.
    }

    # It's not an error for this file to be missing: it might not
    # be readable for various reasons, but for now we ignore that case as well
    # (we've already checked for overly-lax permissions above)
    open my $fh, '<', $pwfile or return undef;

    while (defined(my $line = readline $fh)) {
        next if $line =~ '^#';
        chomp $line;
        my ($host, $port, $db, $user, $pw) = ($line =~ /((?:\\.|[^:])*)(?::|$)/g)
            or next;
        s/\\(.)/$1/g for ($host, $port, $db, $user, $pw);

        return $pw if (
            $host eq '*' || $host eq $self->uri->host and
            $port eq '*' || $port eq $self->uri->port and
            $user eq '*' || $user eq $self->database_user and
            $db   eq '*' || $db   eq $self->database_name
        );
    }

    return undef;
}

sub database_password {
    my $self = shift;
    return $self->uri->password // $ENV{PGPASSWORD} || $self->password_from_file
}

=head2 negotiate_ssl

Apply SSL negotiation.

=cut

async sub negotiate_ssl {
    my ($self, %args) = @_;
    my $stream = delete $args{stream};

    # If SSL is disabled entirely, just return the same stream as-is
    my $ssl = $self->ssl
        or return $stream;

    require IO::Async::SSL;
    require IO::Socket::SSL;

    $log->tracef('Attempting to negotiate SSL');
    await $stream->write($self->protocol->ssl_request);

    $log->tracef('Waiting for response');
    my ($resp, $eof) = await $stream->read_exactly(1);

    $log->tracef('Read %v02x from server for SSL response (EOF is %s)', $resp, $eof ? 'true' : 'false');
    die 'Server closed connection' if $eof;

    if($resp eq 'S') {
        # S for SSL...
        $log->tracef('This is SSL, let us upgrade');
        $stream = await $self->loop->SSL_upgrade(
            handle          => $stream,
            # SSL defaults...
            SSL_server      => 0,
            SSL_hostname    => $self->uri->host,
            SSL_verify_mode => IO::Socket::SSL::SSL_VERIFY_NONE(),
            # Pass through anything SSL-related unchanged, the user knows
            # better than we do
            (map {; $_ => $self->{$_} } grep { /^SSL_/ } keys %$self)
        );
        $log->tracef('Upgrade complete');
    } elsif($resp eq 'N') {
        # N for "no SSL"...
        $log->tracef('No to SSL');
        die 'Server does not support SSL' if $self->ssl == SSL_REQUIRE;
    } else {
        # anything else is unexpected
        die 'Unknown response to SSL request';
    }
    return $stream;
}

sub is_replication { shift->{is_replication} //= 0 }
sub application_name { shift->{application_name} //= 'perl' }

=head2 uri_for_dsn

Returns a L<URI> corresponding to the given L<database source name|https://en.wikipedia.org/wiki/Data_source_name>.

May throw an exception if we don't have a valid string.

=cut

sub uri_for_dsn {
    my ($class, $dsn) = @_;
    die 'invalid DSN, expecting DBI:Pg:...' unless $dsn =~ s/^DBI:Pg://i;
    my %args = split /[=;]/, $dsn;
    my $uri = URI->new('postgresql://postgres@localhost/postgres');
    $uri->$_(delete $args{$_}) for grep exists $args{$_}, qw(host port user password dbname);
    $uri
}

sub uri_for_service {
    my ($class, $service) = @_;
    my $cfg = $class->find_service($service);

    # Start with common default values (i.e. follow libpq behaviour unless there's a strong reason not to)
    my $uri = URI->new('postgresql://postgres@localhost/postgres');

    # Standard fields supported by URI::pg
    $uri->$_(delete $cfg->{$_}) for grep exists $cfg->{$_}, qw(host port user password dbname);
    # ... note that `hostaddr` takes precedence over plain `host`
    $uri->host(delete $cfg->{hostaddr}) if exists $cfg->{hostaddr};

    # Everything else is handled via query parameters, this list is non-exhaustive and likely to be
    # extended in future (e.g. text/binary protocol mode)
    $uri->query_param($_ => delete $cfg->{$_}) for grep exists $cfg->{$_}, qw(
        application_name
        fallback_application_name
        keepalives
        options
        sslmode
        replication
    );
    $uri
}

=head2 stream

The L<IO::Async::Stream> representing the database connection.

=cut

sub stream { shift->{stream} }

=head2 on_read

Process incoming database packets.

Expects the following parameters:

=over 4

=item * C<$stream> - the L<IO::Async::Stream> we are receiving data on

=item * C<$buffref> - a scalar reference to the current input data buffer

=item * C<$eof> - true if we have reached the end of input

=back

=cut

sub on_read {
    my ($self, $stream, $buffref, $eof) = @_;

    try {
        $log->tracef('Have server message of length %d', length $$buffref);
        while(my $msg = $self->protocol->extract_message($buffref)) {
            $log->tracef('Message: %s', $msg);
            $self->incoming->emit($msg);
        }
    } catch($e) {
        # This really shouldn't happen, but since we can't trust our current state we should drop
        # the connection ASAP, and avoid any chance of barrelling through to a COMMIT or other
        # risky operation.
        $log->errorf('Failed to handle read, connection is no longer in a valid state: %s', $e);
        $stream->close_now;
    } finally {
        $self->connected->set_numeric(0) if $eof;
    }
    return 0;
}

=head2 ryu

Provides a L<Ryu::Async> instance.

=cut

sub ryu {
    my ($self) = @_;
    $self->{ryu} //= do {
        $self->add_child(
            my $ryu = Ryu::Async->new
        );
        $ryu
    }
}

=head2 outgoing

L<Ryu::Source> representing outgoing packets for the current database connection.

=cut

sub outgoing {
    my ($self) = @_;
    $self->{outgoing} //= $self->ryu->source;
}

=head2 incoming

L<Ryu::Source> representing incoming packets for the current database connection.

=cut

sub incoming {
    my ($self) = @_;
    $self->{incoming} //= $self->ryu->source;
}

=head2 connected

A L<Ryu::Observable> which will be 1 while the connection is in a valid state,
and 0 if we're disconnected.

=cut

sub connected {
    my ($self) = @_;
    $self->{connected} //= do {
        my $obs = Ryu::Observable->new(0);
        $obs->subscribe(
            $self->$curry::weak(sub {
                my ($self, $v) = @_;
                # We only care about disconnection events
                return if $v;

                # If we were doing something, then it didn't work
                if(my $query = delete $self->{active_query}) {
                    $query->completed->fail('disconnected') unless $query->completed->is_ready;
                }

                # Tell the database pool management that we're no longer useful
                if(my $db = $self->db) {
                    $db->engine_disconnected($self);
                }
            })
        );
        $obs
    };
}

=head2 authenticated

Resolves once database authentication is complete.

=cut

sub authenticated {
    my ($self) = @_;
    $self->{authenticated} //= $self->loop->new_future;
}

# Handlers for authentication messages from backend.
our %AUTH_HANDLER = (
    AuthenticationOk => sub {
        my ($self, $msg) = @_;
        $self->authenticated->done;
    },
    AuthenticationKerberosV5 => sub {
        my ($self, $msg) = @_;
        die "Not yet implemented";
    },
    AuthenticationCleartextPassword => sub {
        my ($self, $msg) = @_;
        $self->protocol->send_message(
            'PasswordMessage',
            user          => $self->encode_text($self->uri->user),
            password_type => 'plain',
            password      => $self->encode_text($self->database_password),
        );
    },
    AuthenticationMD5Password => sub {
        my ($self, $msg) = @_;
        $self->protocol->send_message(
            'PasswordMessage',
            user          => $self->encode_text($self->uri->user),
            password_type => 'md5',
            password_salt => $msg->password_salt,
            password      => $self->encode_text($self->database_password),
        );
    },
    AuthenticationSCMCredential => sub {
        my ($self, $msg) = @_;
        die "Not yet implemented";
    },
    AuthenticationGSS => sub {
        my ($self, $msg) = @_;
        die "Not yet implemented";
    },
    AuthenticationSSPI => sub {
        my ($self, $msg) = @_;
        die "Not yet implemented";
    },
    AuthenticationGSSContinue => sub {
        my ($self, $msg) = @_;
        die "Not yet implemented";
    },
    AuthenticationSASL => sub {
        my ($self, $msg) = @_;
        $log->tracef('SASL starts');
        my $nonce = MIME::Base64::encode_base64(Bytes::Random::Secure::random_string_from(join('', ('a'..'z'), ('A'..'Z'), ('0'..'9')), 18), '');
        $self->{client_first_message} = 'n,,n=,r=' . $nonce;
        $self->protocol->send_message(
            'SASLInitialResponse',
            mechanism        => 'SCRAM-SHA-256',
            nonce            => $nonce,
        );
    },
    AuthenticationSASLContinue => sub {
        my ($self, $msg) = @_;
        $log->tracef('Have msg %s', $msg);

        my $rounds = $msg->password_rounds or die 'need iteration count';

        my $server_first_message = $msg->server_first_message;
        my $pass = Unicode::UTF8::encode_utf8($self->database_password);
        my $salted_password = do {
            my $hash = Crypt::Mac::HMAC::hmac('SHA256', $pass, MIME::Base64::decode_base64($msg->password_salt), pack('N1', 1));
            my $out = $hash;
            # Skip the first round - that's our original $hash value - and recursively re-hash
            # for the remainder, incrementally building our bitwise XOR result
            for my $idx (1..$rounds-1) {
                $hash = Crypt::Mac::HMAC::hmac('SHA256', $pass, $hash);
                $out = "$out" ^ "$hash";
            }
            $out
        };
        # The client key uses the literal string 'Client Key' as the base - for the server, it'd be 'Server Key'
        my $client_key = Crypt::Mac::HMAC::hmac('SHA256', $salted_password, "Client Key");
        my $server_key = Crypt::Mac::HMAC::hmac('SHA256', $salted_password, "Server Key");
        # Then we hash this to get the stored key, which will be used for the signature
        my $stored_key = Crypt::Digest::SHA256::sha256($client_key);

        my $client_first_message = $self->{client_first_message};
        # Strip out the channel-binding GS2 header
        my $header = 'n,,';
        $client_first_message =~ s{^\Q$header}{};
        # ... but we _do_ want the header in the final-message c= GS2 component
        my $client_final_message = 'c=' . MIME::Base64::encode_base64($header, '') . ',r=' . $msg->password_nonce;

        # this is what we want to sign!
        my $auth_message = join ',', $client_first_message, $server_first_message, $client_final_message;
        $log->tracef('Auth message = %s', $auth_message);

        my $client_signature = Crypt::Mac::HMAC::hmac('SHA256', $stored_key, $auth_message);
        my $client_proof = "$client_key" ^ "$client_signature";
        $log->tracef('Client proof is %s', $client_proof);
        my $server_signature = Crypt::Mac::HMAC::hmac('SHA256', $server_key, $auth_message);
        $self->{expected_server_signature} = $server_signature;
        $self->protocol->send_message(
            'SASLResponse',
            header => $header,
            nonce  => $msg->password_nonce,
            proof  => $client_proof,
        );
    },
    AuthenticationSASLFinal => sub {
        my ($self, $msg) = @_;
        my $expected = MIME::Base64::encode_base64($self->{expected_server_signature}, '');
        die 'invalid server signature ' . $msg->server_signature . ', expected ' . $expected unless $msg->server_signature eq $expected;
        $log->tracef('Server signature seems fine, continue with auth');
        # No further action required, we'll get an AuthenticationOk immediately after this
    }
);

=head2 protocol

Returns the L<Protocol::Database::PostgreSQL> instance, creating it
and setting up event handlers if necessary.

=cut

sub protocol {
    my ($self) = @_;
    $self->{protocol} //= do {
        my $pg = Protocol::Database::PostgreSQL::Client->new(
            database => $self->database_name,
            outgoing => $self->outgoing,
        );
        $self->incoming
            ->switch_str(
                sub { $_->type },
                authentication_request => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Auth request received: %s', $msg);
                    my $code = $AUTH_HANDLER{$msg->auth_type}
                        or $log->errorf('unknown auth type %s', $msg->auth_type);
                    $self->$code($msg);
                }),
                password => $self->$curry::weak(sub {
                    my ($self, %args) = @_;
                    $log->tracef('Auth request received: %s', \%args);
                    $self->protocol->{user} = $self->uri->user;
                    $self->protocol->send_message('PasswordMessage', password => $self->encode_text($self->database_password));
                }),
                parameter_status => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Parameter received: %s', $msg);
                    $self->set_parameter(map $self->decode_text($_), $msg->key => $msg->value);
                }),
                row_description => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Row description %s', $msg);
                    $log->errorf('No active query?') unless my $q = $self->active_query;
                    $q->row_description($msg->description);
                }),
                data_row => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Have row data %s', $msg);
                    $self->{fc} ||= $self->active_query->row_data->flow_control->each($self->$curry::weak(sub {
                        my ($self) = @_;
                        $log->tracef('Flow control event - will %s stream', $_ ? 'resume' : 'pause');
                        $self->stream->want_readready($_) if $self->stream;
                    }));
                    $self->active_query->row([ map $self->decode_text($_), $msg->fields ]);
                }),
                command_complete => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    delete $self->{fc};
                    my $query = delete $self->{active_query} or do {
                        $log->warnf('Command complete but no query');
                        return;
                    };
                    $log->tracef('Completed query %s with result %s', $query, $msg->result);
                    $query->done unless $query->completed->is_ready;
                }),
                no_data => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Completed query %s with no data', $self->active_query);
                    # my $query = delete $self->{active_query};
                    # $query->done if $query;
                }),
                send_request => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Send request for %s', $msg);
                    $self->stream->write($msg);
                }),
                ready_for_query => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Ready for query, state is %s', $msg->state);
                    delete $self->{active_query};
                    $self->ready_for_query->set_string($msg->state);
                    $self->db->engine_ready($self) if $self->db;
                }),
                backend_key_data => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Backend key data: pid %d, key 0x%08x', $msg->pid, $msg->key);
                }),
                parse_complete => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Parsing complete for query %s', $self->active_query);
                }),
                bind_complete => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Bind complete for query %s', $self->active_query);
                }),
                close_complete => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    delete $self->{fc};
                    $log->tracef('Close complete for query %s', $self->active_query);
                }),
                empty_query_response => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Query returned no results for %s', $self->active_query);
                }),
                error_response => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    if(my $query = $self->active_query) {
                        $log->warnf('Query returned error %s for %s', $msg->error, $self->active_query);
                        my $f = $query->completed;
                        $f->fail($msg->error) unless $f->is_ready;
                    } else {
                        $log->errorf('Received error %s with no active query', $msg->error);
                    }
                }),
                copy_in_response => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    my $query = $self->active_query;
                    $log->tracef('Ready to copy data for %s', $query);
                    my $proto = $self->protocol;
                    {
                        my $src = $query->streaming_input;
                        $src->completed
                            ->on_ready(sub {
                                my ($f) = @_;
                                $log->tracef('Sending copy done notification, stream status was %s', $f->state);
                                $proto->send_message(
                                    'CopyDone',
                                    data => '',
                                );
                                $proto->send_message(
                                    'Close',
                                    portal    => '',
                                    statement => '',
                                );
                                $proto->send_message(
                                    'Sync',
                                    portal    => '',
                                    statement => '',
                                );
                            });
                            $src->each(sub {
                                $log->tracef('Sending %s', $_);
                                $proto->send_copy_data($_);
                            });
                    }
                    $query->ready_to_stream->done unless $query->ready_to_stream->is_ready;
                }),
                copy_out_response => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('copy out starts %s', $msg);
                    # $self->active_query->row([ $msg->fields ]);
                }),
                copy_data => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Have copy data %s', $msg);
                    my $query = $self->active_query or do {
                        $log->warnf('No active query for copy data');
                        return;
                    };
                    $query->row([ map $self->decode_text($_), @$_ ]) for $msg->rows;
                }),
                copy_done => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    $log->tracef('Copy done - %s', $msg);
                }),
                notification_response => $self->$curry::weak(sub {
                    my ($self, $msg) = @_;
                    my ($chan, $data) = @{$msg}{qw(channel data)};
                    $log->tracef('Notification on channel %s containing %s', $chan, $data);
                    $self->db->notification($self, map $self->decode_text($_), $chan, $data);
                }),
                sub { $log->errorf('Unknown message %s (type %s)', $_, $_->type) }
            );
        $pg
    }
}

sub stream_from {
    my ($self, $src) = @_;
    my $proto = $self->proto;
    $src->each(sub {
        $log->tracef('Sending %s', $_);
        # This is already UTF-8 encoded in the protocol handler,
        # since it's a text-based protocol
        $proto->send_copy_data($_);
    })
}

=head2 set_parameter

Marks a parameter update from the server.

=cut

sub set_parameter {
    my ($self, $k, $v) = @_;
    if(my $param = $self->{parameter}{$k}) {
        $param->set_string($v);
    } else {
        $self->{parameter}{$k} = Ryu::Observable->new($v);
    }
    $self
}

=head2 idle

Resolves when we are idle and ready to process the next request.

=cut

sub idle {
    my ($self) = @_;
    $self->{idle} //= $self->loop->new_future->on_ready(sub {
        delete $self->{idle}
    });
}

sub ready_for_query {
    my ($self) = @_;
    $self->{ready_for_query} //= do {
        Ryu::Observable->new(0)->subscribe($self->$curry::weak(sub {
            my ($self, $v) = @_;
            return unless my $idle = $self->{idle} and $v;
            $idle->done unless $idle->is_ready;
        }))
    }
}

sub simple_query {
    my ($self, $sql) = @_;
    die 'already have active query' if $self->{active_query};
    $self->{active_query} = my $query = Database::Async::Query->new(
        sql      => $sql,
        db       => $self->db,
        row_data => my $src = $self->ryu->source
    );
    $query->completed->on_ready(sub { $src->finish });
    $self->protocol->simple_query($self->encode_text($query->sql));
    return $src;
}

sub encode_text {
    my ($self, $txt) = @_;
    return $txt unless defined $txt and my $encoding = $self->encoding;
    return Unicode::UTF8::encode_utf8($txt) if $encoding eq 'UTF-8';
    return Encode::encode($encoding, $txt, Encode::FB_CROAK);
}

sub decode_text {
    my ($self, $txt) = @_;
    return $txt unless defined $txt and my $encoding = $self->encoding;
    return Unicode::UTF8::decode_utf8($txt) if $encoding eq 'UTF-8';
    return Encode::decode($encoding, $txt, Encode::FB_CROAK);
}

sub handle_query {
    my ($self, $query) = @_;
    die 'already have active query' if $self->{active_query};
    $self->{active_query} = $query;
    my $proto = $self->protocol;
    $proto->send_message(
        'Parse',
        sql       => $self->encode_text($query->sql),
        statement => '',
    );
    $proto->send_message(
        'Bind',
        portal    => '',
        statement => '',
        param     => [ map $self->encode_text($_), $query->bind ],
    );
    $proto->send_message(
        'Describe',
        portal    => '',
        statement => '',
    );
    $proto->send_message(
        'Execute',
        portal    => '',
        statement => '',
    );
    unless($query->{in}) {
        $proto->send_message(
            'Close',
            portal    => '',
            statement => '',
        );
        $proto->send_message(
            'Sync',
            portal    => '',
            statement => '',
        );
    }
    Future->done
}

sub query { die 'use handle_query instead'; }

sub active_query { shift->{active_query} }

=head2 _remove_from_loop

Called when this engine instance is removed from the main event loop, usually just before the instance
is destroyed.

Since we could be in various states of authentication or query processing, we potentially have many
different elements to clean up here. We do these explicitly so that we can apply some ordering to the events:
clear out things that relate to queries before dropping the connection, for example.

=cut

sub _remove_from_loop {
    my ($self, $loop) = @_;
    if(my $query = delete $self->{active_query}) {
        $query->fail('disconnected') unless $query->completed->is_ready;
    }
    if(my $idle = delete $self->{idle}) {
        $idle->cancel;
    }
    if(my $auth = delete $self->{authenticated}) {
        $auth->cancel;
    }
    if(my $connected = delete $self->{connected}) {
        $connected->finish;
    }
    if(my $outgoing = delete $self->{outgoing}) {
        $outgoing->finish unless $outgoing->is_ready;
    }
    if(my $incoming = delete $self->{incoming}) {
        $incoming->finish unless $incoming->is_ready;
    }
    if(my $stream = delete $self->{stream}) {
        $stream->close_now;
        $stream->remove_from_parent;
    }
    if(my $conn = delete $self->{connection}) {
        $conn->cancel;
    }
    # Observable connection parameters - signal that no further updates are expected
    for my $k (keys %{$self->{parameter}}) {
        my $param = delete $self->{parameter}{$k};
        $param->finish if $param;
    }
    if(my $ryu = delete $self->{ryu}) {
        $ryu->remove_from_parent;
    }
    delete $self->{protocol};
    return $self->next::method($loop);
}

1;

__END__

=head1 Implementation notes

Query sequence is essentially:

=over 4

=item * receive C<ReadyForQuery>

=item * send C<frontend_query>

=item * Row Description

=item * Data Row

=item * Command Complete

=item * ReadyForQuery

=back

The DB creates an engine.  The engine does whatever connection handling required, and eventually should reach a "ready" state.
Once this happens, it'll notify DB to say "this engine is ready for queries".
If there are any pending queries, the next in the queue is immediately assigned
to this engine.
Otherwise, the engine is pushed into the pool of available engines, awaiting
query requests.

On startup, the pool `min` count of engine instances will be instantiated.
They start in the pending state.

Any of the following:

=over 4

=item * tx

=item * query

=item * copy etc.

=back

is treated as "queue request". It indicates that we're going to send one or
more commands over a connection.

L</next_engine> resolves with an engine instance:

=over 4

=item * check for engines in `available` queue - these are connected and waiting, and can be assigned immediately

=item * next look for engines in `unconnected` - these are instantiated but need
a ->connection first

=back

=head1 AUTHOR

Tom Molesworth C<< <TEAM@cpan.org> >>

with contributions from Tortsten Förtsch C<< <OPI@cpan.org> >> and Maryam Nafisi C<< <maryam@firstsource.tech> >>.

=head1 LICENSE

Copyright Tom Molesworth 2011-2024. Licensed under the same terms as Perl itself.

