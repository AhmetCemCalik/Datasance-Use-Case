#define _POSIX_C_SOURCE 200809L

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <nats/nats.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <string.h>

typedef struct {
    natsConnection *connection;
    const char *light_id;
    const char *state_subject;
    char current_state[ 16 ];
    struct timespec state_changed_at;
} LightContext;

static volatile sig_atomic_t running = 1;

static void handle_signal( int signal_number )
{
    ( void ) signal_number;
    running = 0;
}

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

static void on_command( natsConnection *nc, natsSubscription *ns, natsMsg *msg, void *closure )
{
    ( void ) nc;
    ( void ) ns;

    const char *subject = natsMsg_GetSubject( msg );
    const char *data = natsMsg_GetData( msg );
    int data_length = natsMsg_GetDataLength( msg );
    LightContext *light_context = closure;

    if ( subject == NULL ) {
        fprintf( stderr, "Command message has no subject.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    if ( data_length < 0 ) {
        fprintf( stderr, "Command payload has an invalid length.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    if ( data_length > 0 && data == NULL ) {
        fprintf( stderr, "Command payload data is missing.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    if ( light_context == NULL ) {
        fprintf( stderr, "Light callback context is missing.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    printf( "Received command, junction=%s, subject=%s, payload=%.*s\n",
            light_context->light_id,
            subject,
            data_length,
            data
    );
    fflush( stdout );

    char payload[ 128 ];

    if ( data_length <= 0 || ( size_t ) data_length >= sizeof( payload ) ) {
        fprintf( stderr, "Command payload is too large or empty for light_id=%s.\n", light_context->light_id );
        natsMsg_Destroy( msg );
        return;
    }

    memcpy( payload, data, ( size_t ) data_length );
    payload[ data_length] = '\0';
    
    char new_state[ 16 ];

    if ( sscanf( payload, "{\"state\":\"%15[^\"]\"}", new_state ) != 1 ) {
        fprintf( stderr, "Invalid incoming message to light_id=%s\n", light_context->light_id );
        natsMsg_Destroy( msg );
        return;
    }

    if ( strcmp( new_state, "GREEN" ) != 0 && strcmp( new_state, "RED" ) != 0 ) {
        fprintf( stderr, "Unknown incoming state=%s to light_id=%s\n", new_state, light_context->light_id );
        natsMsg_Destroy( msg );
        return;
    }

    if ( strcmp( light_context->current_state, new_state ) != 0 ) {
        strcpy( light_context->current_state, new_state );

        if ( clock_gettime( CLOCK_MONOTONIC, &light_context->state_changed_at ) ) {
            perror( "clock_gettime" );
        }
    }

    int written = snprintf( 
        payload, 
        sizeof( payload ), 
        "{\"light_id\":%s,\"state\":\"%s\"}", 
        light_context->light_id,
        light_context->current_state
    );

    if ( written < 0 || ( size_t ) written >= sizeof( payload ) ) {
        fprintf( stderr, "Failed to create command payload.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    natsStatus status = natsConnection_PublishString( light_context->connection, light_context->state_subject, payload);

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_PublishString", status );
        natsMsg_Destroy( msg );
        return;
    }

    printf( "Published state, light_id=%s, subject=%s, payload=%s\n\n",
            light_context->light_id,
            light_context->state_subject,
            payload
    );
    fflush( stdout );
    

    natsMsg_Destroy( msg );
}

int main( void )
{

    natsConnection *connection = NULL;
    natsOptions *options = NULL;
    natsSubscription *subscription = NULL;
    natsStatus status;

    signal( SIGINT, handle_signal );
    signal( SIGTERM, handle_signal );

    const char *light_id = require_env( "LIGHT_ID" );
    const char *command_subject = require_env( "COMMAND_SUBJECT" );
    const char *state_subject = require_env( "STATE_SUBJECT" );
    const char *nats_url = require_env( "NATS_URL" );
    const char *nats_creds = getenv( "NATS_CREDS_PATH" );

    printf( "Traffic light starting.\n" );
    printf( "light_id=%s\n", light_id );
    printf( "command_subject=%s\n", command_subject );
    printf( "state_subject=%s\n", state_subject );
    printf( "nats_url=%s\n", nats_url );

    if ( nats_creds != NULL && nats_creds[0] != '\0' ) {
        printf( "nats_creds=%s\n", nats_creds );
    } else {
        printf( "nats_creds=not configured\n" );
    }

    fflush( stdout );

    status = natsOptions_Create( &options );

    if ( status != NATS_OK ) {
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

    LightContext light_context = { 
        .connection = connection, 
        .light_id = light_id, 
        .state_subject = state_subject, 
        .current_state = "UNKNOWN"
    };

    if ( clock_gettime( CLOCK_MONOTONIC, &light_context.state_changed_at ) != 0 ) {
        perror( "clock_gettime" );
    }

    status = natsConnection_Subscribe( &subscription, connection, command_subject, on_command, &light_context );

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_Subscribe", status );

        natsConnection_Destroy( connection );
        natsOptions_Destroy( options );
        nats_Close();

        return EXIT_FAILURE;
    }

    printf( "Subscribed to command_subject=%s\n", command_subject );
    fflush( stdout );

    while ( running ) {
        sleep( 1 );
    }


    printf( "Traffic light %s is stopping.\n", light_id );

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