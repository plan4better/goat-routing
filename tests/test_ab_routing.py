import concurrent.futures
import time

import psutil
import pytest
import respx
from coordinates import coordinates_list
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response

from src.core.config import settings
from src.endpoints.v2.routing import router
from src.schemas.ab_routing import motis_request_examples

app = FastAPI()
app.include_router(router)
client = TestClient(app)


@respx.mock
def test_compute_ab_routing_success():
    # Mock the external Motis request
    respx.get(settings.MOTIS_PLAN_ENDPOINT).mock(
        return_value=Response(200, json={"routes": [{"id": 1}]})
    )

    response = client.post("/ab-routing", json=motis_request_examples["benchmark"])
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Plan computed successfully."
    assert "result" in data


@respx.mock
def test_compute_ab_routing_motis_error():
    respx.get(settings.MOTIS_PLAN_ENDPOINT).mock(
        return_value=Response(500, text="Internal Server Error")
    )

    response = client.post("/ab-routing", json=motis_request_examples["benchmark"])
    assert response.status_code == 500
    assert "Error from motis service" in response.json()["detail"]


@respx.mock
def test_compute_ab_routing_connection_error():
    import httpx

    respx.get(settings.MOTIS_PLAN_ENDPOINT).mock(
        side_effect=httpx.ConnectError("Cannot connect")
    )

    response = client.post("/ab-routing", json=motis_request_examples["benchmark"])
    assert response.status_code == 503
    assert "Cannot connect to motis service" in response.json()["detail"]


def test_compute_ab_routing_real_motis():
    """
    Integration test: actually calls the Motis service.
    """
    response = client.post("/ab-routing", json=motis_request_examples["default"])

    # Check basic response
    assert response.status_code == 200, f"Motis service returned {response.status_code}"

    data = response.json()
    assert "result" in data
    assert "message" in data
    assert data["message"] == "Plan computed successfully."

    direct_routes = data["result"].get("direct")
    assert isinstance(direct_routes, list)
    assert len(direct_routes) > 0


@pytest.mark.benchmark(group="ab_routing")
@pytest.mark.parametrize("payload_name", list(motis_request_examples.keys()))
def test_benchmark_ab_routing_payloads(benchmark, payload_name):
    """
    Benchmark the AB-routing endpoint using pytest-benchmark for different payloads.
    """
    payload = motis_request_examples[payload_name]

    def send_request():
        response = client.post("/ab-routing", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
        assert data["message"] == "Plan computed successfully."

    # Run the benchmark
    benchmark(send_request)


@pytest.mark.benchmark(group="ab_routing_c")
@pytest.mark.parametrize("payload_name", list(motis_request_examples.keys()))
def test_ab_routing_benchmark_with_resources(benchmark, payload_name):
    """
    Custom benchmark that prints timing and resource usage.
    This does not rely on pytest-benchmark for timing, so we can track CPU/memory per request.
    """

    def send_request():
        num_requests = 15

        payload = motis_request_examples[payload_name]
        timings = []
        cpu_usages = []
        mem_usages = []

        print(f"\nBenchmarking payload '{payload_name}':")

        for _i in range(num_requests):
            process = psutil.Process()
            cpu_before = psutil.cpu_percent(interval=None)
            mem_before = process.memory_info().rss / (1024 * 1024)

            start = time.perf_counter()
            response = client.post("/ab-routing", json=payload)
            end = time.perf_counter()

            cpu_after = psutil.cpu_percent(interval=None)
            mem_after = process.memory_info().rss / (1024 * 1024)

            timings.append((end - start) * 1000)
            cpu_usages.append(cpu_after - cpu_before)
            mem_usages.append(mem_after - mem_before)

            assert response.status_code == 200

        print(
            f"Payload '{payload_name}': Avg time {sum(timings)/len(timings):.2f} ms, "
            f"Avg CPU Δ {sum(cpu_usages)/len(cpu_usages):.2f}%, "
            f"Avg Memory Δ {sum(mem_usages)/len(mem_usages):.2f} MB"
        )

    benchmark(send_request)


@pytest.mark.benchmark(group="ab_routing")
@pytest.mark.parametrize("coord", coordinates_list)
def test_ab_routing_benchmark_with_coords(benchmark, coord):
    def send_requests():
        num_requests = 15
        timings, cpu_usages, mem_usages = [], [], []

        origin, destination = coord
        payload = {
            "fromPlace": origin,
            "toPlace": destination,
            "time": "2025-08-28T08:00:00Z",
        }

        for _ in range(num_requests):
            process = psutil.Process()
            cpu_before = psutil.cpu_percent(interval=None)
            mem_before = process.memory_info().rss / (1024 * 1024)

            start = time.perf_counter()
            response = client.post("/ab-routing", json=payload)
            end = time.perf_counter()

            cpu_after = psutil.cpu_percent(interval=None)
            mem_after = process.memory_info().rss / (1024 * 1024)

            timings.append((end - start) * 1000)
            cpu_usages.append(cpu_after - cpu_before)
            mem_usages.append(mem_after - mem_before)

            assert response.status_code == 200

        return {
            "avg_time": sum(timings) / len(timings),
            "avg_cpu": sum(cpu_usages) / len(cpu_usages),
            "avg_mem": sum(mem_usages) / len(mem_usages),
        }

    result = benchmark(send_requests)
    print(f"Route {coord[0]} -> {coord[1]}: {result}")


def test_heavy_load_concurrent(num_clients=200):
    """
    Simulate many clients calling the same route concurrently and compute average response time.
    """
    timings = []

    def call_motis(idx: int):
        """
        Function to call Motis and return elapsed time.
        """
        start = time.perf_counter()
        response = client.post("/ab-routing", json=motis_request_examples["benchmark"])
        end = time.perf_counter()
        elapsed_ms = (end - start) * 1000

        assert (
            response.status_code == 200
        ), f"Status {response.status_code} for client {idx}"
        data = response.json()
        assert "result" in data
        assert "message" in data and data["message"] == "Plan computed successfully."

        return elapsed_ms

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(call_motis, i + 1) for i in range(num_clients)]
        for future in concurrent.futures.as_completed(futures):
            timings.append(future.result())

    avg_time = sum(timings) / len(timings) if timings else 0
    print(f"\nHeavy load test completed for {num_clients} clients.")
    print(f"Average response time: {avg_time:.2f} ms")
