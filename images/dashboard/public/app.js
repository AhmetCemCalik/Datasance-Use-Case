const lights = new Map();
const observations = new Map();

const emptyState =
    document.querySelector("#empty-state");

const connectionStatus =
    document.querySelector("#connection-status");

function formatElapsed(changedAt) {
    if (typeof changedAt !== "number") {
        return "0.00";
    }

    const elapsed =
        Math.max(0, Date.now() - changedAt) / 1000;

    return elapsed.toFixed(2);
}

function getLightSlot(lightId) {
    return document.getElementById(
        `light-${lightId}-slot`
    );
}

function getLightElement(lightId) {
    const slot = getLightSlot(lightId);

    if (!slot) {
        return null;
    }

    return slot.querySelector(
        `[data-light-id="${lightId}"]`
    );
}

function getMapMarker(lightId) {
    return document.querySelector(
        `.map-light[data-map-light-id="${lightId}"]`
    );
}

function updateEmptyState() {
    emptyState.hidden =
        lights.size > 0 ||
        observations.size > 0;
}

function createLightElement(lightId) {
    const slot = getLightSlot(lightId);

    if (!slot) {
        console.error(
            `Missing slot for light ${lightId}`
        );

        return null;
    }

    const article =
        document.createElement("article");

    article.className = "light-card";
    article.dataset.lightId = lightId;
    article.dataset.state = "UNKNOWN";

    article.innerHTML = `
        <div class="light-heading">
            <h2>Light ${lightId}</h2>
            <span class="state-label">
                UNKNOWN
            </span>
        </div>

        <dl>
            <div>
                <dt>State</dt>
                <dd class="state-value">
                    UNKNOWN
                </dd>
            </div>

            <div>
                <dt>Vehicles</dt>
                <dd class="vehicle-count">
                    —
                </dd>
            </div>

            <div>
                <dt>Elapsed</dt>
                <dd>
                    <span class="elapsed-value">
                        0.00
                    </span>
                    s
                </dd>
            </div>
        </dl>
    `;

    slot.append(article);

    return article;
}

function ensureLightElement(lightId) {
    let article = getLightElement(lightId);

    if (!article) {
        article = createLightElement(lightId);
    }

    return article;
}

function renderLight(light) {
    lights.set(light.lightId, light);
    updateEmptyState();

    const article =
        ensureLightElement(light.lightId);

    if (!article) {
        return;
    }

    article.dataset.state = light.state;

    article.querySelector(
        ".state-label"
    ).textContent = light.state;

    article.querySelector(
        ".state-value"
    ).textContent = light.state;

    article.querySelector(
        ".elapsed-value"
    ).textContent = formatElapsed(
        light.changedAt
    );

    const marker = getMapMarker(light.lightId);

    if (marker) {
        marker.dataset.state = light.state;
    }
}

function renderObservation(observation) {
    observations.set(
        observation.observationId,
        observation
    );

    updateEmptyState();

    const article = ensureLightElement(
        observation.observationId
    );

    if (!article) {
        return;
    }

    article.querySelector(
        ".vehicle-count"
    ).textContent =
        observation.vehicleCount;
}

function updateElapsedTimes() {
    for (const light of lights.values()) {
        const article =
            getLightElement(light.lightId);

        if (!article) {
            continue;
        }

        article.querySelector(
            ".elapsed-value"
        ).textContent = formatElapsed(
            light.changedAt
        );
    }

    requestAnimationFrame(
        updateElapsedTimes
    );
}

async function loadInitialLights() {
    const response =
        await fetch("/api/lights");

    if (!response.ok) {
        throw new Error(
            `Initial light request failed: ` +
            `${response.status}`
        );
    }

    const initialLights =
        await response.json();

    for (const light of initialLights) {
        renderLight(light);
    }
}

async function loadInitialObservations() {
    const response =
        await fetch("/api/observations");

    if (!response.ok) {
        throw new Error(
            `Initial observation request failed: ` +
            `${response.status}`
        );
    }

    const initialObservations =
        await response.json();

    for (
        const observation
        of initialObservations
    ) {
        renderObservation(observation);
    }
}

function connectEvents() {
    console.log("Creating EventSource");

    const events =
        new EventSource("/events");

    events.addEventListener("open", () => {
        console.log("SSE opened");

        connectionStatus.textContent = "Live";
        connectionStatus.dataset.connected =
            "true";
    });

    events.addEventListener(
        "light-state",
        event => {
            console.log(
                "Received light-state:",
                event.data
            );

            try {
                const light =
                    JSON.parse(event.data);

                renderLight(light);
            } catch (error) {
                console.error(error);
            }
        }
    );

    events.addEventListener(
        "traffic-observation",
        event => {
            console.log(
                "Received traffic-observation:",
                event.data
            );

            try {
                const observation =
                    JSON.parse(event.data);

                renderObservation(observation);
            } catch (error) {
                console.error(error);
            }
        }
    );

    events.addEventListener(
        "snapshot",
        event => {
            console.log(
                "Received light snapshot:",
                event.data
            );

            try {
                const snapshot =
                    JSON.parse(event.data);

                for (const light of snapshot) {
                    renderLight(light);
                }
            } catch (error) {
                console.error(error);
            }
        }
    );

    events.addEventListener(
        "observation-snapshot",
        event => {
            console.log(
                "Received observation snapshot:",
                event.data
            );

            try {
                const snapshot =
                    JSON.parse(event.data);

                for (
                    const observation
                    of snapshot
                ) {
                    renderObservation(
                        observation
                    );
                }
            } catch (error) {
                console.error(error);
            }
        }
    );

    events.addEventListener(
        "error",
        event => {
            console.error(
                "SSE error",
                event
            );

            connectionStatus.textContent =
                "Reconnecting";

            connectionStatus.dataset.connected =
                "false";
        }
    );
}

try {
    await Promise.all([
        loadInitialLights(),
        loadInitialObservations()
    ]);
} catch (error) {
    console.error(error);
}

connectEvents();
updateElapsedTimes();