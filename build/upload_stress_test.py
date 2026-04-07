from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import math
import os
import pathlib
import shutil
import socket
import statistics
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from waitress.server import create_server


ROOT = pathlib.Path(__file__).resolve().parents[1]
APP_PATH = sorted(ROOT.glob("*.py"), key=lambda path: path.stat().st_size, reverse=True)[0]


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    return int(raw)


SMALL_FILE_REQUESTS = env_int("LANFS_SMALL_FILE_REQUESTS", 20)
SMALL_FILES_PER_REQUEST = env_int("LANFS_SMALL_FILES_PER_REQUEST", 4)
SMALL_FILE_SIZE = env_int("LANFS_SMALL_FILE_SIZE", 128 * 1024)

CHUNK_SIZE = env_int("LANFS_CHUNK_SIZE", 10 * 1024 * 1024)
LARGE_UPLOAD_CLIENTS = env_int("LANFS_LARGE_UPLOAD_CLIENTS", 8)
LARGE_UPLOAD_FILE_SIZE = env_int("LANFS_LARGE_UPLOAD_FILE_SIZE", 40 * 1024 * 1024)

RESUME_FILE_SIZE = env_int("LANFS_RESUME_FILE_SIZE", 35 * 1024 * 1024)
RESUME_INITIAL_CHUNKS = env_int("LANFS_RESUME_INITIAL_CHUNKS", 2)

DOWNLOAD_LOAD_CLIENTS = env_int("LANFS_DOWNLOAD_LOAD_CLIENTS", 8)
DOWNLOAD_LOAD_FILE_SIZE = env_int("LANFS_DOWNLOAD_LOAD_FILE_SIZE", 256 * 1024 * 1024)
UPLOAD_UNDER_LOAD_CLIENTS = env_int("LANFS_UPLOAD_UNDER_LOAD_CLIENTS", 4)
UPLOAD_UNDER_LOAD_FILE_SIZE = env_int("LANFS_UPLOAD_UNDER_LOAD_FILE_SIZE", 30 * 1024 * 1024)


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * ratio) - 1))
    return ordered[index]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def load_module():
    module = importlib.util.module_from_spec(
        importlib.util.spec_from_loader("lanfs_upload_stress", loader=None)
    )
    module.__file__ = str(APP_PATH)

    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        source = APP_PATH.read_text(encoding="utf-8-sig")
        code = compile(source, str(APP_PATH), "exec")
        exec(code, module.__dict__)

    app = getattr(module, "app", None)
    if app is None:
        raise RuntimeError("Flask app object was not found")

    app.testing = False
    return module, app


def configure_isolated_runtime(module, temp_root: pathlib.Path) -> pathlib.Path:
    shared_dir = temp_root / "shared"
    temp_upload_dir = temp_root / "temp_uploads"
    shared_dir.mkdir(parents=True, exist_ok=True)
    temp_upload_dir.mkdir(parents=True, exist_ok=True)

    module.UPLOAD_FOLDER = str(shared_dir)
    module.UPLOAD_FOLDER_ABS = os.path.abspath(str(shared_dir))
    module.TEMP_FOLDER = str(temp_upload_dir)
    module.TASKS_FILE = str(temp_root / "tasks.json")
    module.SHARE_LINKS_FILE = str(temp_root / "share_links.json")
    module.ADMIN_REQUESTS_FILE = str(temp_root / "admin_requests.json")

    module.app.config["UPLOAD_FOLDER"] = module.UPLOAD_FOLDER

    with module.tasks_lock:
        module.tasks.clear()
        module.save_tasks()

    with module.user_lock:
        module.online_users.clear()

    with module.activity_lock:
        module.current_activities.clear()

    with module.prepared_download_tasks_lock:
        module.prepared_download_tasks.clear()

    return shared_dir


class WaitressHarness:
    def __init__(self, app, threads: int, connection_limit: int, channel_timeout: int, cleanup_interval: int):
        self.port = free_port()
        self.server = create_server(
            app,
            host="127.0.0.1",
            port=self.port,
            threads=threads,
            connection_limit=connection_limit,
            channel_timeout=channel_timeout,
            cleanup_interval=cleanup_interval,
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        self.thread.start()
        deadline = time.time() + 5.0
        while time.time() < deadline:
            try:
                requests.get(self.base_url + "/", timeout=0.5)
                return
            except Exception:
                time.sleep(0.1)
        raise RuntimeError("Waitress server did not start in time")

    def stop(self) -> None:
        self.server.close()
        self.thread.join(timeout=5.0)


def make_bytes(size: int, seed: int) -> bytes:
    pattern = f"seed-{seed}-".encode("utf-8")
    repeated = pattern * ((size // len(pattern)) + 1)
    return repeated[:size]


def collect_latency_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"count": 0, "p50_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0, "mean_ms": 0.0}
    return {
        "count": len(values),
        "p50_ms": round(percentile(values, 0.50) * 1000, 1),
        "p95_ms": round(percentile(values, 0.95) * 1000, 1),
        "max_ms": round(max(values) * 1000, 1),
        "mean_ms": round(statistics.mean(values) * 1000, 1),
    }


def scenario_small_file_uploads(base_url: str, shared_dir: pathlib.Path) -> dict[str, object]:
    latencies: list[float] = []
    failures: list[str] = []

    def worker(index: int) -> float:
        files = []
        for inner in range(SMALL_FILES_PER_REQUEST):
            name = f"small_{index}_{inner}.txt"
            files.append(("files", (name, make_bytes(SMALL_FILE_SIZE, index * 100 + inner), "text/plain")))
        start = time.perf_counter()
        response = requests.post(base_url + "/api/upload_files", files=files, timeout=60)
        elapsed = time.perf_counter() - start
        payload = response.json()
        if response.status_code != 200 or not payload.get("success"):
            raise AssertionError(f"small upload failed: {response.status_code} {payload}")
        return elapsed

    with ThreadPoolExecutor(max_workers=SMALL_FILE_REQUESTS) as executor:
        futures = [executor.submit(worker, i) for i in range(SMALL_FILE_REQUESTS)]
        for future in as_completed(futures):
            try:
                latencies.append(future.result())
            except Exception as exc:
                failures.append(str(exc))

    actual_count = len([item for item in shared_dir.iterdir() if item.is_file() and item.name.startswith("small_")])
    expected_count = SMALL_FILE_REQUESTS * SMALL_FILES_PER_REQUEST

    return {
        "scenario": "small_file_uploads",
        "requests": SMALL_FILE_REQUESTS,
        "files_per_request": SMALL_FILES_PER_REQUEST,
        "file_size_bytes": SMALL_FILE_SIZE,
        "successes": len(latencies),
        "failures": len(failures),
        "expected_files": expected_count,
        "actual_files": actual_count,
        "total_bytes": expected_count * SMALL_FILE_SIZE,
        "throughput_mb_s": round(((expected_count * SMALL_FILE_SIZE) / 1024 / 1024) / max(sum(latencies), 0.001), 2),
        "latency": collect_latency_stats(latencies),
        "failure_samples": failures[:5],
    }


def upload_chunked_file(
    base_url: str,
    filename: str,
    file_size: int,
    seed: int,
    start_chunk: int = 0,
    stop_after_chunks: int | None = None,
    task_id: str | None = None,
) -> dict[str, object]:
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    if not task_id:
        task_id = str(uuid.uuid4())
    latencies: list[float] = []
    sent_chunks = 0

    for chunk_index in range(start_chunk, total_chunks):
        if stop_after_chunks is not None and sent_chunks >= stop_after_chunks:
            break
        start = chunk_index * CHUNK_SIZE
        end = min(file_size, start + CHUNK_SIZE)
        chunk_bytes = make_bytes(end - start, seed + chunk_index)
        form = {
            "task_id": task_id,
            "chunk_index": str(chunk_index),
            "total_chunks": str(total_chunks),
            "filename": filename,
            "upload_path": "",
        }
        files = {"chunk": (f"{filename}.part{chunk_index}", chunk_bytes, "application/octet-stream")}
        started = time.perf_counter()
        response = requests.post(base_url + "/api/upload_chunk", data=form, files=files, timeout=180)
        elapsed = time.perf_counter() - started
        payload = response.json()
        if response.status_code != 200 or not payload.get("success"):
            raise AssertionError(f"chunk upload failed at {chunk_index}: {response.status_code} {payload}")
        latencies.append(elapsed)
        sent_chunks += 1

    return {
        "task_id": task_id,
        "total_chunks": total_chunks,
        "latencies": latencies,
        "sent_chunks": sent_chunks,
    }


def scenario_large_chunk_uploads(base_url: str, shared_dir: pathlib.Path) -> dict[str, object]:
    completion_times: list[float] = []
    chunk_latencies: list[float] = []
    failures: list[str] = []

    def worker(index: int) -> float:
        filename = f"large_{index}.bin"
        start = time.perf_counter()
        result = upload_chunked_file(base_url, filename, LARGE_UPLOAD_FILE_SIZE, 1000 + index)
        elapsed = time.perf_counter() - start
        chunk_latencies.extend(result["latencies"])
        output_path = shared_dir / filename
        if not output_path.exists() or output_path.stat().st_size != LARGE_UPLOAD_FILE_SIZE:
            raise AssertionError(f"merged file invalid: {filename}")
        return elapsed

    with ThreadPoolExecutor(max_workers=LARGE_UPLOAD_CLIENTS) as executor:
        futures = [executor.submit(worker, i) for i in range(LARGE_UPLOAD_CLIENTS)]
        for future in as_completed(futures):
            try:
                completion_times.append(future.result())
            except Exception as exc:
                failures.append(str(exc))

    total_bytes = LARGE_UPLOAD_CLIENTS * LARGE_UPLOAD_FILE_SIZE
    total_duration = max(completion_times) if completion_times else 0.001
    return {
        "scenario": "large_chunk_uploads",
        "clients": LARGE_UPLOAD_CLIENTS,
        "file_size_bytes": LARGE_UPLOAD_FILE_SIZE,
        "chunk_size_bytes": CHUNK_SIZE,
        "successes": len(completion_times),
        "failures": len(failures),
        "aggregate_throughput_mb_s": round((total_bytes / 1024 / 1024) / max(total_duration, 0.001), 2),
        "file_completion": collect_latency_stats(completion_times),
        "chunk_latency": collect_latency_stats(chunk_latencies),
        "failure_samples": failures[:5],
    }


def scenario_resume_upload(base_url: str, shared_dir: pathlib.Path) -> dict[str, object]:
    filename = "resume_case.bin"
    task_id = filename.rsplit(".", 1)[0]

    first_stage = upload_chunked_file(
        base_url,
        filename,
        RESUME_FILE_SIZE,
        2000,
        start_chunk=0,
        stop_after_chunks=RESUME_INITIAL_CHUNKS,
        task_id=task_id,
    )

    remaining_start = first_stage["sent_chunks"]
    total_chunks = first_stage["total_chunks"]
    latencies = list(first_stage["latencies"])

    for chunk_index in range(remaining_start, total_chunks):
        start_offset = chunk_index * CHUNK_SIZE
        end_offset = min(RESUME_FILE_SIZE, start_offset + CHUNK_SIZE)
        chunk_bytes = make_bytes(end_offset - start_offset, 2000 + chunk_index)
        form = {
            "task_id": task_id,
            "chunk_index": str(chunk_index),
            "total_chunks": str(total_chunks),
            "filename": filename,
            "upload_path": "",
        }
        files = {"chunk": (f"{filename}.part{chunk_index}", chunk_bytes, "application/octet-stream")}
        started = time.perf_counter()
        response = requests.post(base_url + "/api/upload_chunk", data=form, files=files, timeout=180)
        elapsed = time.perf_counter() - started
        payload = response.json()
        if response.status_code != 200 or not payload.get("success"):
            raise AssertionError(f"resume chunk failed at {chunk_index}: {response.status_code} {payload}")
        latencies.append(elapsed)

    output_path = shared_dir / filename
    with open(output_path, "rb") as handle:
        observed_hash = __import__("hashlib").md5(handle.read()).hexdigest()
    expected_bytes = bytearray()
    for chunk_index in range(total_chunks):
        start_offset = chunk_index * CHUNK_SIZE
        end_offset = min(RESUME_FILE_SIZE, start_offset + CHUNK_SIZE)
        expected_bytes.extend(make_bytes(end_offset - start_offset, 2000 + chunk_index))
    expected_hash = __import__("hashlib").md5(bytes(expected_bytes)).hexdigest()

    return {
        "scenario": "resume_upload",
        "initial_chunks_sent": RESUME_INITIAL_CHUNKS,
        "total_chunks": total_chunks,
        "success": output_path.exists() and output_path.stat().st_size == RESUME_FILE_SIZE and observed_hash == expected_hash,
        "latency": collect_latency_stats(latencies),
        "observed_md5": observed_hash,
        "expected_md5": expected_hash,
    }


def consume_download(base_url: str, filename: str) -> float:
    started = time.perf_counter()
    with requests.get(base_url + f"/download/{filename}", stream=True, timeout=300) as response:
        if response.status_code != 200:
            raise AssertionError(f"download load request failed: {response.status_code}")
        for _chunk in response.iter_content(chunk_size=1024 * 1024):
            if not _chunk:
                continue
    return time.perf_counter() - started


def scenario_upload_under_download_load(base_url: str, shared_dir: pathlib.Path) -> dict[str, object]:
    load_file = shared_dir / "download_load_source.bin"
    with open(load_file, "wb") as handle:
        remaining = DOWNLOAD_LOAD_FILE_SIZE
        seed = 9000
        while remaining > 0:
            size = min(CHUNK_SIZE, remaining)
            handle.write(make_bytes(size, seed))
            remaining -= size
            seed += 1

    download_times: list[float] = []
    upload_completion_times: list[float] = []
    upload_failures: list[str] = []
    chunk_latencies: list[float] = []

    download_executor = ThreadPoolExecutor(max_workers=DOWNLOAD_LOAD_CLIENTS)
    download_futures = [download_executor.submit(consume_download, base_url, load_file.name) for _ in range(DOWNLOAD_LOAD_CLIENTS)]

    time.sleep(0.5)

    def upload_worker(index: int) -> float:
        filename = f"load_upload_{index}.bin"
        started = time.perf_counter()
        result = upload_chunked_file(base_url, filename, UPLOAD_UNDER_LOAD_FILE_SIZE, 3000 + index)
        elapsed = time.perf_counter() - started
        chunk_latencies.extend(result["latencies"])
        output_path = shared_dir / filename
        if not output_path.exists() or output_path.stat().st_size != UPLOAD_UNDER_LOAD_FILE_SIZE:
            raise AssertionError(f"upload under load invalid: {filename}")
        return elapsed

    with ThreadPoolExecutor(max_workers=UPLOAD_UNDER_LOAD_CLIENTS) as executor:
        futures = [executor.submit(upload_worker, i) for i in range(UPLOAD_UNDER_LOAD_CLIENTS)]
        for future in as_completed(futures):
            try:
                upload_completion_times.append(future.result())
            except Exception as exc:
                upload_failures.append(str(exc))

    for future in as_completed(download_futures):
        download_times.append(future.result())
    download_executor.shutdown(wait=True)

    total_upload_bytes = UPLOAD_UNDER_LOAD_CLIENTS * UPLOAD_UNDER_LOAD_FILE_SIZE
    upload_duration = max(upload_completion_times) if upload_completion_times else 0.001
    return {
        "scenario": "upload_under_download_load",
        "download_clients": DOWNLOAD_LOAD_CLIENTS,
        "download_file_size_bytes": DOWNLOAD_LOAD_FILE_SIZE,
        "upload_clients": UPLOAD_UNDER_LOAD_CLIENTS,
        "upload_file_size_bytes": UPLOAD_UNDER_LOAD_FILE_SIZE,
        "upload_successes": len(upload_completion_times),
        "upload_failures": len(upload_failures),
        "upload_aggregate_throughput_mb_s": round((total_upload_bytes / 1024 / 1024) / max(upload_duration, 0.001), 2),
        "upload_file_completion": collect_latency_stats(upload_completion_times),
        "upload_chunk_latency": collect_latency_stats(chunk_latencies),
        "download_completion": collect_latency_stats(download_times),
        "failure_samples": upload_failures[:5],
    }


def main() -> None:
    module, app = load_module()
    with tempfile.TemporaryDirectory(prefix="lanfs-upload-stress-") as temp_dir:
        temp_root = pathlib.Path(temp_dir)
        shared_dir = configure_isolated_runtime(module, temp_root)

        harness = WaitressHarness(
            app,
            threads=module.WAITRESS_THREADS,
            connection_limit=module.WAITRESS_CONNECTION_LIMIT,
            channel_timeout=module.WAITRESS_CHANNEL_TIMEOUT,
            cleanup_interval=module.WAITRESS_CLEANUP_INTERVAL,
        )
        harness.start()
        try:
            started = time.perf_counter()
            results = {
                "base_url": harness.base_url,
                "waitress_threads": module.WAITRESS_THREADS,
                "connection_limit": module.WAITRESS_CONNECTION_LIMIT,
                "small_file_uploads": scenario_small_file_uploads(harness.base_url, shared_dir),
                "large_chunk_uploads": scenario_large_chunk_uploads(harness.base_url, shared_dir),
                "resume_upload": scenario_resume_upload(harness.base_url, shared_dir),
                "upload_under_download_load": scenario_upload_under_download_load(harness.base_url, shared_dir),
                "elapsed_seconds": round(time.perf_counter() - started, 2),
            }
            print(json.dumps(results, ensure_ascii=False, indent=2))
        finally:
            harness.stop()
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    main()
