from datetime import datetime

import httpx
from coordinates import coordinates_list
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.core.config import settings
from src.endpoints.v2.routing import router

app = FastAPI()
app.include_router(router)
client = TestClient(app)

TIME_BENCH = "2025-08-28T08:00:00Z"


def get_motis_route(origin, destination, time=TIME_BENCH):
    """
    Query the Motis API for a route between origin and destination.
    """
    motis_payload = {
        "fromPlace": origin,
        "toPlace": destination,
        "time": time,
    }

    try:
        response = client.post("/ab-routing", json=motis_payload)
        return response.json()
    except Exception as e:
        print(f"Error calling AB-routing for {origin} -> {destination}: {e}")
        return None


def extract_motis_route_summary(result):
    """
    Extract only the first route summary from a Motis API response.
    """
    routes = result.get("result", {}).get("itineraries", [])
    if not routes:
        return None

    route = routes[0]  # take first option only

    # Total duration
    duration = route.get(
        "duration", sum(leg.get("duration", 0) for leg in route.get("legs", []))
    )

    # Total distance
    distance = 0
    for leg in route.get("legs", []):
        if "distance" in leg:
            distance += leg["distance"]
        elif "summary" in leg:
            distance += leg["summary"].get("distance", 0) or leg["summary"].get(
                "length", 0
            )

    # Modes and vehicle lines
    modes, vehicle_lines = [], []
    for leg in route.get("legs", []):
        if "mode" in leg:
            modes.append(leg["mode"])
        elif "transport_mode" in leg:
            modes.append(leg["transport_mode"])
        elif "transports" in leg:
            for t in leg["transports"]:
                modes.append(t.get("mode", "unknown"))
        else:
            modes.append("unknown")

        route_name = leg.get("routeShortName")
        if route_name:
            vehicle_lines.append(route_name)

    return {
        "duration": duration,
        "distance": distance,
        "modes": modes,
        "vehicle_lines": vehicle_lines,
    }


def get_google_directions(
    origin,
    destination,
    mode="transit",
    time=TIME_BENCH,
    api_key=str(settings.GOOGLE_API_KEY),
):
    """
    Calls Google Directions API using httpx and returns the JSON response.
    Includes error handling for API call failures.
    """
    # Convert ISO8601 string to UNIX timestamp
    dt = datetime.fromisoformat(time.replace("Z", "+00:00"))
    departure_timestamp = int(dt.timestamp())
    params = {
        "origin": origin,
        "destination": destination,
        "mode": mode,
        "departure_time": departure_timestamp,
        "key": api_key,
    }
    url = str(settings.GOOGLE_DIRECTIONS_URL)

    try:
        with httpx.Client() as client_http:
            response = client_http.get(url, params=params)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        print(f"HTTP Error occurred while calling Google Directions API: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def extract_google_route_summary(directions_result):
    """
    Extract only the first route summary from a Google Directions API response.
    """
    routes = directions_result.get("routes", [])
    if not routes:
        return None

    legs = routes[0].get("legs", [])
    if not legs:
        return None

    leg = legs[0]  # take first leg
    duration = leg["duration"]["value"]
    distance = leg["distance"]["value"]

    modes, vehicle_lines = [], []
    for step in leg.get("steps", []):
        transit_details = step.get("transit_details", {})
        line_info = transit_details.get("line", {})
        vehicle_type = line_info.get("vehicle", {}).get("type")
        line_name = line_info.get("short_name")

        if vehicle_type:
            modes.append(vehicle_type)
            if line_name:
                vehicle_lines.append(line_name)
        else:
            modes.append(step.get("travel_mode", "UNKNOWN"))

    return {
        "duration": duration,
        "distance": distance,
        "modes": modes,
        "vehicle_lines": vehicle_lines,
    }


def run_benchmark():
    print(
        f"{'Route':<5}| {'Motis Dur(s)':<12} | {'Google Dur(s)':<13} | "
        f"{'Motis Dist(m)':<14} | {'Google Dist(m)':<14} | "
        f"{'Motis Modes':<20} | {'Google Modes':<20} | "
        f"{'Motis Vehicles':<20} | {'Google Vehicles'}"
    )
    print("-" * 200)

    for idx, (origin, destination) in enumerate(coordinates_list, start=1):
        motis_result = get_motis_route(origin, destination)
        motis_route = (
            extract_motis_route_summary(motis_result) if motis_result else None
        )
        if not motis_route:
            motis_route = {
                "duration": "-",
                "distance": "-",
                "modes": [],
                "vehicle_lines": [],
            }

        google_result = get_google_directions(origin, destination)
        google_route = (
            extract_google_route_summary(google_result) if google_result else None
        )
        if not google_route:
            google_route = {
                "duration": "-",
                "distance": "-",
                "modes": [],
                "vehicle_lines": [],
            }

        print(
            f"{idx:<5} |"
            f"{motis_route['duration']:<12} | {google_route['duration']:<13} | "
            f"{motis_route['distance']:<14} | {google_route['distance']:<14} | "
            f"{','.join(motis_route['modes']):<20} | {','.join(google_route['modes']):<20} | "
            f"{','.join(motis_route['vehicle_lines']):<20} | {','.join(google_route['vehicle_lines'])}"
        )


if __name__ == "__main__":
    run_benchmark()
