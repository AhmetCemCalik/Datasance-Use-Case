/*
0.1.6 -> New scheduler rule
0.1.7 -> Update for generator
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include <nats/nats.h>

typedef struct {
    natsConnection *connection;
    const char *observer_id;
    const char *observation_subject;
} ObserverContext;

static volatile sig_atomic_t running = 1;

// Atomic termination
static void handle_signal( int signal_number )
{
    ( void ) signal_number;
    running = 0;
}

// Helper to get environment value, exit and return if value null
static const char *require_env( const char *name )
{
    const char *value = getenv( name );

    if ( value == NULL || value[0] == '\0' ) {
        fprintf( stderr, "Missing environment value %s.\n", name );
        exit( EXIT_FAILURE );
    }

    return value;
}

static void print_nats_error( const char *operation, natsStatus status )
{
    fprintf( stderr, "%s failed: %s\n", operation, natsStatus_GetText( status ) );
}

static int parse_integer_field( const char *payload, const char *field_name, long *value )
{
    const char *field = strstr( payload, field_name );

    if ( field == NULL ) {
        return -1;
    }

    field += strlen( field_name );

    errno = 0;
    char *end = NULL;

    long parsed = strtol( field, &end, 10 );

    if ( errno == ERANGE || end == field ) {
        return -1;
    }

    *value = parsed;
    return 0;
}

static void on_generated_traffic( natsConnection *nc, natsSubscription *ns, natsMsg *msg, void *closure )
{
    ( void ) nc;
    ( void ) ns;
    ObserverContext *context = closure;

    if (
        context == NULL ||
        context->connection == NULL ||
        context->observation_subject == NULL ||
        context->observer_id == NULL
    ) {
        fprintf( stderr, "Generated traffic callback received invalid context.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    const char *subject = natsMsg_GetSubject( msg );
    const char *data = natsMsg_GetData( msg );
    int data_length = natsMsg_GetDataLength( msg );

    if ( subject == NULL ) {
        fprintf( stderr, "Generated traffic message has no subject.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    if ( data_length <= 0 ) {
        fprintf( stderr, "Generated traffic payload has an invalid length.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    if ( data_length > 0 && data == NULL ) {
        fprintf( stderr, "Generated traffic payload data is missing.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    
    char incoming_payload[ 256 ];

    if ( ( size_t ) data_length >= sizeof( incoming_payload ) ) {
        fprintf( stderr, "Generated traffic payload is too large.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    memcpy( incoming_payload, data, ( size_t ) data_length );
    incoming_payload[ data_length ] = '\0';

    long vehicle_count;
    long sequence;

    if ( parse_integer_field( incoming_payload, "\"vehicle_count\":", &vehicle_count ) != 0 ) {
        fprintf( stderr, "Could not parse vehicle count from payload: %s\n", incoming_payload );
        natsMsg_Destroy( msg );
        return;
    }
    if ( parse_integer_field( incoming_payload, "\"sequence\":", &sequence ) != 0 ) {
        fprintf( stderr, "Could not parse sequence from payload: %s\n", incoming_payload );
        natsMsg_Destroy( msg );
        return;
    }
    if ( vehicle_count < 0 || sequence < 0 ) {
        fprintf( stderr, "Incoming payload has invalid values: %s\n", incoming_payload );
        natsMsg_Destroy( msg );
        return;
    }

    char observation_payload[ 256 ];

    int written = snprintf(
        observation_payload,
        sizeof( observation_payload ),
        "{"
            "\"observer_id\":\"%s\","
            "\"vehicle_count\":%ld,"
            "\"sequence\":%ld"
        "}",
        context->observer_id,
        vehicle_count,
        sequence
    );

    if ( written < 0 || ( size_t ) written >= sizeof( observation_payload ) ) {
        fprintf( stderr, "Failed to create observation payload.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    natsStatus status = natsConnection_PublishString( context->connection, context->observation_subject, observation_payload );

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_PublishString", status );
        natsMsg_Destroy( msg );
        return;
    }

    status = natsConnection_FlushTimeout(
        context->connection,
        2000
    );

    if ( status != NATS_OK ) {
        print_nats_error(
            "natsConnection_FlushTimeout",
            status
        );

        natsMsg_Destroy( msg );
        return;
    }

    printf(
        "Observed generated traffic, "
        "generation_subject=%s, "
        "observation_subject=%s, "
        "payload=%s\n",
        subject,
        context->observation_subject,
        observation_payload
    );

    fflush( stdout );
    natsMsg_Destroy( msg );
}

int main( void )
{
    natsConnection *connection = NULL;
    natsSubscription *subscription = NULL;
    natsOptions *options = NULL;
    natsStatus status;

    signal( SIGINT, handle_signal );
    signal( SIGTERM, handle_signal );

    const char *observer_id = require_env( "OBSERVER_ID" );
    const char *observation_subject = require_env( "OBSERVATION_SUBJECT" );
    const char *generation_subject = require_env( "GENERATION_SUBJECT" );
    const char *nats_url = require_env( "NATS_URL" );
    const char *nats_creds = getenv( "NATS_CREDS_PATH" );

    printf( "Traffic observer starting.\n" );
    printf( "observer_id=%s\n", observer_id );
    printf( "observation_subject=%s\n", observation_subject );
    printf( "generation_subject=%s\n", generation_subject );
    printf( "nats_url=%s\n", nats_url );

    if ( nats_creds != NULL && nats_creds[0] != '\0' ) {
        printf( "nats_creds=%s\n", nats_creds );
    } else {
        printf( "nats_creds=not configured\n" );
    }

    fflush( stdout );

    status = natsOptions_Create( &options );

    if (status != NATS_OK) {
        print_nats_error( "natsOptions_Create", status );
        natsOptions_Destroy( options );
        return EXIT_FAILURE;
    }

    status = natsOptions_SetURL( options, nats_url );

    if ( status != NATS_OK ) {
        print_nats_error( "natsOptions_SetURL", status );
        natsOptions_Destroy( options );
        return EXIT_FAILURE;
    }

    if ( nats_creds != NULL && nats_creds[0] != '\0' ) {
        status = natsOptions_SetUserCredentialsFromFiles( options, nats_creds, NULL );

        if ( status != NATS_OK ) {
            print_nats_error( "natsOptions_SetUserCredentialsFromFiles", status );

            natsOptions_Destroy( options );
            return EXIT_FAILURE;
        }
    }

    status = natsConnection_Connect( &connection, options );

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_Connect", status );
        natsOptions_Destroy( options );
        return EXIT_FAILURE;
    }

    printf( "Connected to NATS.\n" );
    fflush( stdout );

    ObserverContext observer_context = { 
        .connection = connection, 
        .observer_id = observer_id,
        .observation_subject = observation_subject 
    };

    status = natsConnection_Subscribe( &subscription, connection, generation_subject, on_generated_traffic, &observer_context );

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_Subscribe", status );

        natsConnection_Destroy( connection );
        natsOptions_Destroy( options );
        nats_Close();

        return EXIT_FAILURE;
    }

    printf( "Subscribed to generation_subject=%s\n", generation_subject );
    fflush( stdout );

    while ( running ) {
        sleep( 1 );
    }

    printf( "Traffic observer %s is stopping.\n", observer_id );
    fflush( stdout );

    if ( subscription != NULL ) {
        natsSubscription_Destroy( subscription );
    }

    if ( connection != NULL ) {
        natsConnection_FlushTimeout( connection, 1000 );
        natsConnection_Destroy( connection );
    }

    natsOptions_Destroy( options );
    nats_Close();

    return EXIT_SUCCESS;
}