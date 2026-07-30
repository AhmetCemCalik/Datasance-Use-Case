/*
0.1.8 -> First version
1.0.0 -> Final version constant tuning
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <stdatomic.h>
#include <string.h>
#include <errno.h>
#include <nats/nats.h>

#define CARS_PER_GREEN_INTERVAL 15

typedef struct {
    atomic_int *light_1_is_green;
    atomic_int *light_2_is_green;
    atomic_int *light_3_is_green;
} LightStateContext;

static atomic_int light_1_is_green = 0;
static atomic_int light_2_is_green = 1;
static atomic_int light_3_is_green = 1;

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

static void on_light_state( natsConnection *nc, natsSubscription *ns, natsMsg *msg, void *closure )
{
    ( void ) nc;
    ( void ) ns;
    LightStateContext *light_context = closure;

    if (
        light_context == NULL ||
        light_context->light_1_is_green == NULL ||
        light_context->light_2_is_green == NULL ||
        light_context->light_3_is_green == NULL
    ) {
        fprintf( stderr, "Light-state callback received invalid context.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    const char *subject = natsMsg_GetSubject( msg );
    const char *data = natsMsg_GetData( msg );
    int data_length = natsMsg_GetDataLength( msg );

    if ( subject == NULL ) {
        fprintf( stderr, "Light state message has no subject.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    if ( data_length < 0 ) {
        fprintf( stderr, "Light state payload has an invalid length.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    if ( data_length > 0 && data == NULL ) {
        fprintf( stderr, "Light state payload data is missing.\n" );
        natsMsg_Destroy( msg );
        return;
    }
    
    char payload[ 128 ];

    if ( ( size_t ) data_length >= sizeof( payload ) ) {
        fprintf( stderr, "Light-state payload is too large.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    memcpy( payload, data, ( size_t ) data_length );
    payload[ data_length ] = '\0';

    int is_green;

    if ( strstr( payload, "\"state\":\"GREEN\"" ) != NULL ) {
        is_green = 1;
    } else if ( strstr( payload, "\"state\":\"RED\"") != NULL ) {
        is_green = 0;
    } else {
        fprintf( stderr, "Failed to get the light state from payload %s.\n", payload );
        natsMsg_Destroy( msg );
        return;
    }

    const char *light_id_text = strrchr( subject, '.' );

    if ( light_id_text == NULL || light_id_text[1] == '\0' ) {
        fprintf( stderr, "Invalid light-state subject: %s\n", subject );
        natsMsg_Destroy(msg);
        return;
    }

    light_id_text++;

    errno = 0;
    char *end = NULL;

    long light_id = strtol(light_id_text, &end, 10);

    if ( errno == ERANGE || end == light_id_text || *end != '\0' || light_id < 1 || light_id > 3 ) {
        fprintf( stderr, "Invalid light ID in subject: %s\n", subject );
        natsMsg_Destroy(msg);
        return;
    }

    switch ( light_id ) {
        case 1:
            atomic_store( light_context->light_1_is_green, is_green );
            break;

        case 2:
            atomic_store( light_context->light_2_is_green, is_green );
            break;

        case 3:
            atomic_store( light_context->light_3_is_green, is_green );
            break;
    }

    printf( "Updated Light %ld state to %s.\n", light_id, is_green ? "GREEN" : "RED" );

    fflush( stdout );
    natsMsg_Destroy( msg );
}

static int remove_cars( int *queue, int maximum )
{
    int removed = *queue < maximum ? *queue : maximum;

    *queue -= removed;

    return removed;
}

static void simulate_interval( int *q1, int *q2, int *q3, int light_1_is_green, int light_2_is_green, int light_3_is_green )
{
    int light_1_arriving = ( rand() % 10 ) + 1;
    int light_3_arriving = ( rand() % 10 ) + 1;

    *q1 += light_1_arriving;
    *q3 += light_3_arriving;

    printf( "%d cars arrived at Light 1. " "%d cars arrived at Light 3.\n", light_1_arriving, light_3_arriving );

    if ( light_1_is_green ) {
        int passed = remove_cars( q1, CARS_PER_GREEN_INTERVAL );

        printf( "%d cars passed Light 1.\n", passed );
    }

    if ( light_2_is_green ) {
        int passed = remove_cars( q2, CARS_PER_GREEN_INTERVAL );

        printf( "%d cars passed Light 2.\n", passed );
    }

    if ( light_3_is_green ) {
        int transferred = remove_cars( q3, CARS_PER_GREEN_INTERVAL );

        *q2 += transferred;

        printf( "%d cars passed Light 3 and arrived at Light 2.\n", transferred );
    }

    printf( "Queues: Light 1=%d, Light 2=%d, Light 3=%d\n", *q1, *q2, *q3 );

    fflush( stdout );
}

static int publish_queue_count(
    natsConnection *connection,
    const char *subject_prefix,
    int light_id,
    int vehicle_count,
    unsigned int sequence
)
{
    if ( connection == NULL || subject_prefix == NULL || light_id < 1 || light_id > 3 || vehicle_count < 0 ) {
        fprintf(stderr, "publish_queue_count received invalid arguments.\n" );
        return -1;
    }

    char subject[ 256 ];

    int written = snprintf( subject, sizeof( subject ), "%s.%d", subject_prefix, light_id );

    if ( written < 0 || ( size_t ) written >= sizeof( subject ) ) {
        fprintf( stderr, "Failed to create generation subject for Light %d.\n", light_id );
        return -1;
    }

    char payload[ 256 ];

    written = snprintf(
        payload,
        sizeof( payload ),
        "{"
            "\"light_id\":%d,"
            "\"vehicle_count\":%d,"
            "\"sequence\":%u"
        "}",
        light_id,
        vehicle_count,
        sequence
    );

    if ( written < 0 || ( size_t ) written >= sizeof( payload ) ) {
        fprintf( stderr, "Failed to create generation payload for Light %d.\n", light_id );
        return -1;
    }

    natsStatus status = natsConnection_PublishString( connection, subject, payload );

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_PublishString", status );
        return -1;
    }

    printf( "Published generated traffic, light=%d, subject=%s, payload=%s\n", light_id, subject, payload );

    fflush( stdout );

    return 0;
}

int main( void )
{
    srand( ( unsigned int ) time( NULL ) );
    
    natsConnection *connection = NULL;
    natsOptions *options = NULL;
    natsSubscription *subscription = NULL;
    natsStatus status;

    signal( SIGINT, handle_signal );
    signal( SIGTERM, handle_signal );

    const char *generation_subject = require_env( "GENERATION_SUBJECT" );
    const char *state_subject = require_env( "STATE_SUBJECT" );
    const char *nats_url = require_env( "NATS_URL" );
    const char *nats_creds = getenv( "NATS_CREDS_PATH" );

    printf( "Generator starting.\n" );
    printf( "generation_subject=%s\n", generation_subject );
    printf( "state_subject=%s\n", state_subject );
    printf( "nats_url=%s\n", nats_url );

    if ( nats_creds != NULL && nats_creds[0] != '\0' ) {
        printf( "nats_creds=%s\n", nats_creds );
    } else {
        printf( "nats_creds=not configured\n" );
    }

    fflush( stdout );

    // Create the nats connection
    status = natsOptions_Create( &options );

    if ( status != NATS_OK ) {
        print_nats_error( "natsOptions_Create", status );
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

    LightStateContext light_context = { 
        .light_1_is_green = &light_1_is_green, 
        .light_2_is_green = &light_2_is_green, 
        .light_3_is_green = &light_3_is_green
    };

    status = natsConnection_Subscribe( &subscription, connection, state_subject, on_light_state, &light_context );

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_Subscribe", status );

        natsConnection_Destroy( connection );
        natsOptions_Destroy( options );
        nats_Close();

        return EXIT_FAILURE;
    }

    printf( "Subscribed to state_subject=%s\n", state_subject );
    fflush( stdout );

    int q1 = 0;
    int q2 = 0;
    int q3 = 0;

    unsigned int sequence = 0;

    while ( running ) {
        int current_light_1_is_green = atomic_load( &light_1_is_green );

        int current_light_2_is_green = atomic_load( &light_2_is_green );

        int current_light_3_is_green = atomic_load( &light_3_is_green );

        printf(
            "Current states: Light 1=%s, Light 2=%s, Light 3=%s\n",
            current_light_1_is_green ? "GREEN" : "RED",
            current_light_2_is_green ? "GREEN" : "RED",
            current_light_3_is_green ? "GREEN" : "RED"
        );

        simulate_interval( &q1, &q2, &q3, current_light_1_is_green, current_light_2_is_green, current_light_3_is_green );

        sequence++;

        int publish_1 = publish_queue_count( connection, generation_subject, 1, q1, sequence );

        int publish_2 = publish_queue_count( connection, generation_subject, 2, q2, sequence );

        int publish_3 = publish_queue_count( connection, generation_subject, 3, q3, sequence );

        if ( publish_1 != 0 || publish_2 != 0 || publish_3 != 0 ) {
            fprintf( stderr, "Failed to publish one or more generated queue counts.\n" );
            break;
        }

        status = natsConnection_FlushTimeout( connection, 2000 );

        if ( status != NATS_OK ) {
            print_nats_error( "natsConnection_FlushTimeout", status );
            break;
        }

        for (
            int elapsed = 0;
            elapsed < 10 && running;
            elapsed++
        ) {
            sleep( 1 );
        }
    }

    printf( "Generator is stopping.\n" );
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