import express from "express";
import { connect, credsAuthenticator } from "nats";
import fs from "node:fs";

const HTTP_PORT = Number.parseInt(
    process.env.HTTP_PORT ?? "8080",
    10
);

const NATS_URL = process.env.NATS_URL;
const NATS_CREDS_PATH = process.env.NATS_CREDS_PATH;
const STATE_SUBJECT = process.env.STATE_SUBJECT ?? "traffic.state.*";
const OBSERVATION_SUBJECT = process.env.OBSERVATION_SUBJECT ?? "traffic.observation.*";

if ( !NATS_URL ) {
    console.error( "Missing environment value NATS_URL" );
    process.exit( 1 );
}

if ( !Number.isInteger( HTTP_PORT ) || HTTP_PORT <= 0 ) {
    console.error( `Invalid HTTP_PORT=${ process.env.HTTP_PORT }` );
    process.exit( 1 );
}

const app = express();
const clients = new Set();
const lights = new Map();
const observations = new Map();

app.use( express.static( "public" ) );

app.get( "/api/lights", ( request, response ) => {
    response.json( Array.from( lights.values() ) );
} );

app.get( "/api/observations", ( request, response ) => {
    response.json( Array.from( observations.values() ) );
} );

app.get( "/events", ( request, response ) => {
    response.setHeader( "Content-Type", "text/event-stream" );
    response.setHeader( "Cache-Control", "no-cache" );
    response.setHeader( "Connection", "keep-alive" );
    response.flushHeaders();

    clients.add( response );
    console.log(`Browser connected. Clients = ${clients.size}`);

    response.write(
        `event: snapshot\ndata: ${JSON.stringify(
            Array.from( lights.values() )
        ) }\n\n`
    );
    response.write(
        `event: observation-snapshot\n` +
        `data: ${ JSON.stringify(
            Array.from( observations.values() )
        ) }\n\n`
    );

    request.on( "close", () => {
        clients.delete( response );

        console.log(
            `Browser disconnected. Clients = ${clients.size}`
        );
    } );
} );

function broadcast( eventName, value ) {
    const encoded = JSON.stringify( value );

    for ( const client of clients) {
        client.write( 
            `event: ${eventName}\ndata: ${encoded}\n\n` 
        );
    }
}

function parseLightId( subject ) {
    const seperator = subject.lastIndexOf( "." );

    if ( seperator < 0 || seperator == subject.length - 1 ) {
        return null;
    }

    const lightId = subject.slice( seperator + 1 );

    if ( !/^[1-9][0-9]*$/.test( lightId ) ) {
        return null;
    }

    return lightId;
}

function parseObservationId( subject ) {
    const seperator = subject.lastIndexOf( "." );

    if ( seperator < 0 || seperator == subject.length - 1 ) {
        return null;
    }

    const observationId = subject.slice( seperator + 1 );

    if ( !/^[1-9][0-9]*$/.test( observationId ) ) {
        return null;
    }

    return observationId;
}

function parseStateMessage( subject, data ) {
    const lightIdFromSubject = parseLightId( subject );

    if ( lightIdFromSubject === null ) {
        throw new Error( `Invalid state subject: ${ subject }` );
    }

    const parsed = JSON.parse( data );

    if (
        parsed.state !== "GREEN" &&
        parsed.state !== "RED"
    ) {
        throw new Error( `Invalid light state: ${parsed.state}` );
    }

    if (
        parsed.light_id !== undefined &&
        String( parsed.light_id ) !== lightIdFromSubject
    ) {
        throw new Error(
            `Light ID mismatch: subject=${ lightIdFromSubject }, ` +
            `payload=${ parsed.light_id }`
        );
    }

    return {
        lightId: lightIdFromSubject,
        state: parsed.state
    };
}

function parseObservationMessage( subject, data ) {
    const observationIdFromSubject = parseObservationId( subject );

    if ( observationIdFromSubject === null ) {
        throw new Error( `Invalid observation subject: ${ subject }` );
    }

    const parsed = JSON.parse( data );

    const vehicleCount = Number( parsed.vehicle_count );

    if ( !Number.isInteger( vehicleCount ) || vehicleCount < 0 ) {
        throw new Error( `Invalid vehicle_count: ${ parsed.vehicle_count }` );
    }

    if (
        parsed.observer_id !== undefined && String( parsed.observer_id ) !== observationIdFromSubject ) {
        throw new Error(
            `Observer ID mismatch: ` +
            `subject=${ observationIdFromSubject }, ` +
            `payload=${ parsed.observer_id }`
        );
    }

    return { observationId: observationIdFromSubject, vehicleCount };
}

async function start() {
    const connectionOptions = {
        servers: NATS_URL
    };

    if (
        NATS_CREDS_PATH &&
        NATS_CREDS_PATH.length > 0
    ) {
        connectionOptions.authenticator =
            credsAuthenticator(
                fs.readFileSync( NATS_CREDS_PATH )
            );
    }

    const natsConnection = await connect( connectionOptions );

    console.log( `Connected to NATS at ${ natsConnection.getServer() }` );

    const lightSubscription = natsConnection.subscribe( STATE_SUBJECT );

    console.log( `Subscribed to state subject ${ STATE_SUBJECT }` );

    void ( async () => {
        for await ( const message of lightSubscription ) {
            try {
                const stateReport = parseStateMessage(
                    message.subject,
                    message.string()
                );

                const existing =
                    lights.get( stateReport.lightId );

                const stateChanged =
                    existing === undefined ||
                    existing.state !== stateReport.state;

                const light = {
                    lightId: stateReport.lightId,
                    state: stateReport.state,
                    changedAt: stateChanged
                        ? Date.now()
                        : existing.changedAt,
                    lastSeenAt: Date.now()
                };

                lights.set( light.lightId, light );
                broadcast( "light-state", light );

                console.log(
                    `State received: light_id=${ light.lightId }, ` +
                    `state=${ light.state }`
                );
            } catch ( error ) {
                console.error(
                    `Rejected state message on ` +
                    `${ message.subject }: ${ error.message }`
                );
            }
        }
    })();

    const observationSubscription = natsConnection.subscribe( OBSERVATION_SUBJECT );

    console.log( `Subscribed to observation subject ${ OBSERVATION_SUBJECT }` )

    void (async () => {
        for await ( const message of observationSubscription ) {
            try {
                const observationReport =
                    parseObservationMessage( message.subject, message.string() );

                const existing = observations.get( observationReport.observationId );

                const countChanged = existing === undefined || existing.vehicleCount !== observationReport.vehicleCount;

                const now = Date.now();

                const observation = {
                    observationId:
                        observationReport.observationId,

                    vehicleCount:
                        observationReport.vehicleCount,

                    changedAt: countChanged
                        ? now
                        : existing.changedAt,

                    lastSeenAt: now
                };

                observations.set( observation.observationId, observation );

                broadcast( "traffic-observation", observation );

                console.log(
                    `Observation received: ` +
                    `observer_id=${ observation.observationId }, ` +
                    `vehicle_count=${ observation.vehicleCount }`
                );
            } catch ( error ) {
                console.error(
                    `Rejected observation message on ` +
                    `${ message.subject }: ${ error.message }`
                );
            }
        }
    } )();

    app.listen( HTTP_PORT, "0.0.0.0", () => {
        console.log(
            `Dashboard listening on port ${ HTTP_PORT }`
        );
    });

    const shutdown = async () => {
        console.log( "Dashboard stopping." );

        lightSubscription.unsubscribe();
        observationSubscription.unsubscribe();
        await natsConnection.drain();
        process.exit( 0 );
    };

    process.on( "SIGINT", shutdown );
    process.on( "SIGTERM", shutdown );
}

start().catch(( error ) => {
    console.error( "Dashboard startup failed:", error );
    process.exit( 1 );
});