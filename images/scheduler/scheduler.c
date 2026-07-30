/* 
0.1.5 -> New scheduling rule
0.1.6 -> Scheduling rule fix
0.1.7 -> Update for generator
0.1.8 -> 3rd light fix
1.0.0 -> Final with constant tweaks
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <nats/nats.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>

#define LIGHT_SWITCH_INTERVAL_SECONDS 40
#define LIGHT_2_VEHICLE_THRESHOLD 20

typedef struct {
    natsConnection *connection;
    const char *command_subject_prefix;
} SchedulerContext;

static volatile sig_atomic_t running = 1;

static atomic_int observer_2_vehicle_count = 0;

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

static int publish_light_state( SchedulerContext *context, int light_id, const char *state )
{
    if ( context == NULL || context->connection == NULL || context->command_subject_prefix == NULL ) {
        fprintf( stderr, "Scheduler callback received invalid context.\n" );
        return -1;
    }

    char command_subject[ 256 ];

    int written = snprintf( command_subject, sizeof( command_subject), "%s.%d", context->command_subject_prefix, light_id );

    if ( written < 0 || ( size_t ) written >= sizeof( command_subject ) ) {
        fprintf( stderr, "Failed to create a command subject.\n" );
        return -1;
    }

    char command_payload[ 64 ];

    written = snprintf( command_payload, sizeof( command_payload ), "{\"state\":\"%s\"}", state );

    if ( written < 0 || ( size_t ) written >= sizeof( command_payload ) ) {
        fprintf( stderr, "Failed to create a command payload for light %d.\n", light_id );
        return -1;
    }

    natsStatus status = natsConnection_PublishString( context->connection, command_subject, command_payload );

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_PublishString", status );
        return -1;
    }

    printf( "Published command, light_id=%d, subject=%s, payload=%s\n",
            light_id,
            command_subject,
            command_payload
    );
    fflush( stdout );

    return 0;
}

static void on_observation( natsConnection *nc, natsSubscription *ns, natsMsg *msg, void *closure)
{
    ( void ) nc;
    ( void ) ns;
    SchedulerContext *context = closure;

    if ( context == NULL || context->connection == NULL || context->command_subject_prefix == NULL ) {
        fprintf( stderr, "Scheduler callback received invalid context.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    const char *subject = natsMsg_GetSubject( msg );
    const char *data = natsMsg_GetData( msg );
    int data_length = natsMsg_GetDataLength( msg );

    if ( subject == NULL ) {
        fprintf( stderr, "Observation message has no subject.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    if ( data_length < 0 ) {
        fprintf( stderr, "Observation payload has an invalid length.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    if ( data_length <= 0 || data == NULL ) {
        fprintf( stderr, "Observation payload data is missing.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    char incoming_payload[ 256 ];

    if ( ( size_t ) data_length >= sizeof( incoming_payload ) ) {
        fprintf( stderr, "Observation payload is too large.\n" );
        natsMsg_Destroy( msg );
        return;
    }

    memcpy( incoming_payload, data, ( size_t ) data_length );
    incoming_payload[ data_length ] = '\0';

    long parsed_vehicle_count;

    if ( parse_integer_field( incoming_payload, "\"vehicle_count\":", &parsed_vehicle_count ) != 0 ) {
        fprintf( stderr, "Could not parse vehicle count from payload: %s\n", incoming_payload );
        natsMsg_Destroy( msg );
        return;
    }

    if ( parsed_vehicle_count < 0 || parsed_vehicle_count > INT_MAX ) {
        fprintf( stderr, "Vehicle count is outside of the valid range: %ld\n", parsed_vehicle_count );
        natsMsg_Destroy( msg );
        return;
    }
    int vehicle_count = ( int ) parsed_vehicle_count;

    const char *junction_id_str = strrchr( subject, '.' );

    if ( junction_id_str == NULL || junction_id_str[1] == '\0' ) {
        fprintf( stderr, "Invalid observation subject %s\n", subject );

        natsMsg_Destroy( msg );
        return;
    }

    junction_id_str++;

    errno = 0;
    char *end = NULL;

    long junction_id = strtol( junction_id_str, &end, 10 );

    if ( errno == ERANGE || end == junction_id_str || *end != '\0' || junction_id <= 0 || junction_id > INT_MAX ) {
        fprintf( stderr, "Invalid junction ID in subject %s\n", subject );

        natsMsg_Destroy( msg );
        return;
    }

    printf( "Received observation, junction=%ld, subject=%s, payload=%.*s\n",
        junction_id,
        subject,
        data_length,
        data
    );
    fflush( stdout );

    if ( junction_id == 2 ) {
        atomic_store( &observer_2_vehicle_count, vehicle_count );
        printf( "Updated observer 2 vehicle count to %d.\n", vehicle_count );

        fflush( stdout );
    }

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

    const char *observation_subject = require_env( "OBSERVATION_SUBJECT" );
    // const char *light_state_subject = require_env( "LIGHT_STATE_SUBJECT" );
    const char *command_subject_prefix = require_env( "COMMAND_SUBJECT_PREFIX" );
    const char *nats_url = require_env( "NATS_URL" );
    const char *nats_creds = getenv( "NATS_CREDS_PATH" );


    printf( "Scheduler starting.\n" );
    printf( "observation_subject=%s\n", observation_subject );
    // printf( "light_state_subject=%s\n", light_state_subject );
    printf( "command_subject_prefix=%s\n", command_subject_prefix );
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

    SchedulerContext context = { .connection = connection, .command_subject_prefix = command_subject_prefix};

    status = natsConnection_Subscribe( &subscription, connection, observation_subject, on_observation, &context );

    if ( status != NATS_OK ) {
        print_nats_error( "natsConnection_Subscribe", status );

        natsConnection_Destroy( connection );
        natsOptions_Destroy( options );
        nats_Close();

        return EXIT_FAILURE;
    }

    printf( "Subscribed to observation_subject=%s\n", observation_subject );
    fflush( stdout );

    /*
    * Initial states:
    *
    * Light 1 = RED
    * Light 2 = GREEN
    *
    * Light 3 is determined by Observer 2's vehicle count.
    */
    int light_2_is_green = 1;

    /*
    * -1 forces the scheduler to publish Light 3's initial state
    * during the first loop iteration.
    */
    int previous_light_3_is_green = -1;

    int elapsed_seconds = 0;

    int light_1_result = publish_light_state( &context, 1, light_2_is_green ? "RED" : "GREEN" );

    int light_2_result = publish_light_state( &context, 2, light_2_is_green ? "GREEN" : "RED" );

    if ( light_1_result != 0 || light_2_result != 0 ) {
        fprintf( stderr, "Failed to publish one or more initial light states.\n" );
    }

    /*
    * Ensure that initial commands have been transmitted before
    * entering the scheduling loop.
    */
    status = natsConnection_FlushTimeout(
        connection,
        2000
    );

    if ( status != NATS_OK ) {
        print_nats_error( "Initial natsConnection_FlushTimeout", status );
    }

    while ( running ) {
        /*
        * Observer 2 reports the queue currently waiting at Light 2.
        */
        int current_light_2_vehicle_count = atomic_load( &observer_2_vehicle_count );

        /*
        * Stop traffic entering Light 2 from Light 3 when the
        * Light 2 queue reaches the threshold.
        */
        int desired_light_3_is_green = current_light_2_vehicle_count < LIGHT_2_VEHICLE_THRESHOLD;

        /*
        * Publish only when Light 3 needs to change.
        *
        * previous_light_3_is_green starts at -1, so the initial
        * Light 3 state is always published.
        */
        if ( desired_light_3_is_green != previous_light_3_is_green
        ) {
            const char *desired_light_3_state = desired_light_3_is_green ? "GREEN" : "RED";

            if ( publish_light_state( &context, 3, desired_light_3_state ) == 0 ) {
                /*
                * Update this only after successful publication.
                * If publication fails, the scheduler retries during
                * the next loop iteration.
                */
                previous_light_3_is_green = desired_light_3_is_green;

                printf(
                    "Light 3 decision: "
                    "light_2_vehicle_count=%d, "
                    "threshold=%d, "
                    "state=%s\n",
                    current_light_2_vehicle_count,
                    LIGHT_2_VEHICLE_THRESHOLD,
                    desired_light_3_state
                );

                fflush( stdout );
            }
        }

        /*
        * Wait one second so Light 3 responds reasonably quickly
        * without using a busy loop.
        */
        sleep( 1 );

        if ( !running ) {
            break;
        }

        elapsed_seconds++;

        /*
        * Lights 1 and 2 switch antagonistically every 60 seconds.
        */
        if ( elapsed_seconds >= LIGHT_SWITCH_INTERVAL_SECONDS ) {
            light_2_is_green = !light_2_is_green;

            const char *light_1_state = light_2_is_green ? "RED" : "GREEN";

            const char *light_2_state = light_2_is_green ? "GREEN" : "RED";

            light_1_result = publish_light_state( &context, 1, light_1_state );

            light_2_result = publish_light_state( &context, 2, light_2_state );

            if ( light_1_result != 0 || light_2_result != 0 ) {
                fprintf( stderr, "Failed to publish one or more " "scheduled light states.\n" );
            } else {
                printf( "Scheduled switch: " "Light 1=%s, Light 2=%s\n", light_1_state, light_2_state );
                fflush( stdout );
            }

            /*
            * Begin the next 60-second period.
            *
            * Reset this regardless of publication success so a NATS
            * failure does not cause commands to be sent every second.
            */
            elapsed_seconds = 0;
        }
    }

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