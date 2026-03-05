from __future__ import annotations

import json
import logging
import os
import socket
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import docker
import requests
from requests.auth import HTTPBasicAuth


LOG = logging.getLogger("telegram_alert_bot")

STATE_EMOJI = {
    "failed": "❌",
    "success": "✅",
    "running": "🟡",
    "healthy": "✅",
    "unhealthy": "❌",
    "missing": "❌",
}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        LOG.warning("Invalid integer for %s=%r, using %s", name, raw, default)
        return default


def env_csv(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


def env_value(name: str, default: str = "") -> str:
    file_path = os.getenv(f"{name}_FILE", "").strip()
    if file_path:
        try:
            return Path(file_path).read_text().strip()
        except Exception as exc:
            LOG.warning("Failed to read secret file for %s at %s: %s", name, file_path, exc)
    return os.getenv(name, default)


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: dict[str, Any] = {"dag_failures": {}, "dag_status": {}, "services": {}}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            self.data = json.loads(self.path.read_text())
        except Exception as exc:
            LOG.warning("Failed to load state file %s: %s", self.path, exc)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True))


class TelegramAlerter:
    def __init__(self, token: str, chat_id: str, timeout: int = 15) -> None:
        self.token = token
        self.chat_id = chat_id
        self.timeout = timeout

    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def send(self, text: str) -> None:
        if not self.enabled():
            LOG.warning("Telegram is not configured. Skipping alert: %s", text.splitlines()[0])
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        resp = requests.post(
            url,
            timeout=self.timeout,
            json={"chat_id": self.chat_id, "text": text[:4000]},
        )
        resp.raise_for_status()


class AirflowMonitor:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        project_root: Path,
        max_dags: int,
        max_log_lines: int,
        request_timeout: int,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(username, password)
        self.project_root = project_root
        self.max_dags = max_dags
        self.max_log_lines = max_log_lines
        self.timeout = request_timeout
        self.session = requests.Session()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, auth=self.auth, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def list_failed_runs(self) -> list[dict[str, Any]]:
        dags_payload = self._get("/api/v1/dags", params={"limit": self.max_dags, "only_active": "true"})
        failed_runs: list[dict[str, Any]] = []
        for dag in dags_payload.get("dags", []):
            dag_id = dag["dag_id"]
            runs_payload = self._get(
                f"/api/v1/dags/{quote(dag_id, safe='')}/dagRuns",
                params={"limit": 5, "order_by": "-start_date"},
            )
            for run in runs_payload.get("dag_runs", []):
                if run.get("state") == "failed":
                    failed_runs.append({"dag_id": dag_id, "run": run})
        return failed_runs

    def list_latest_runs(self) -> list[dict[str, Any]]:
        dags_payload = self._get("/api/v1/dags", params={"limit": self.max_dags, "only_active": "true"})
        latest_runs: list[dict[str, Any]] = []
        for dag in dags_payload.get("dags", []):
            dag_id = dag["dag_id"]
            runs_payload = self._get(
                f"/api/v1/dags/{quote(dag_id, safe='')}/dagRuns",
                params={"limit": 1, "order_by": "-start_date"},
            )
            runs = runs_payload.get("dag_runs", [])
            if runs:
                latest_runs.append({"dag_id": dag_id, "run": runs[0]})
        return latest_runs

    def build_failure_message(self, dag_id: str, run: dict[str, Any]) -> str:
        run_id = run["dag_run_id"]
        payload = self._get(
            f"/api/v1/dags/{quote(dag_id, safe='')}/dagRuns/{quote(run_id, safe='')}/taskInstances"
        )
        failed_tasks = [task for task in payload.get("task_instances", []) if task.get("state") == "failed"]
        lines = [
            "❌ AIRFLOW DAG FAILED",
            f"dag_id: {dag_id}",
            f"run_id: {run_id}",
            f"state: {emoji_for(run.get('state'))} {run.get('state')}",
            f"start_date: {run.get('start_date')}",
            f"end_date: {run.get('end_date')}",
        ]
        if failed_tasks:
            lines.append("failed_tasks:")
            for task in failed_tasks[:3]:
                lines.append(
                    f"- {task.get('task_id')} state={emoji_for(task.get('state'))} {task.get('state')} try_number={task.get('try_number')}"
                )
        return "\n".join(lines)

    def _read_failed_task_log(self, dag_id: str, run_id: str, task_id: str) -> list[str]:
        task_dir = (
            self.project_root
            / "airflow"
            / "logs"
            / f"dag_id={dag_id}"
            / f"run_id={run_id}"
            / f"task_id={task_id}"
        )
        if not task_dir.exists():
            return []
        attempt_logs = sorted(task_dir.glob("attempt=*.log"))
        if not attempt_logs:
            return []
        try:
            content = attempt_logs[-1].read_text(errors="replace").splitlines()
        except Exception as exc:
            return [f"<failed to read log: {exc}>"]
        tail = content[-self.max_log_lines :]
        return tail


class DockerMonitor:
    def __init__(self, project_name: str, monitored_services: list[str], max_log_lines: int) -> None:
        self.project_name = project_name
        self.monitored_services = set(monitored_services)
        self.max_log_lines = max_log_lines
        self.client = docker.from_env()

    def collect_failures(self) -> list[dict[str, str]]:
        failures: list[dict[str, str]] = []
        service_map = self._containers_by_service()
        for service_name in sorted(self.monitored_services):
            container = service_map.get(service_name)
            if not container:
                failures.append(
                    {
                        "service": service_name,
                        "fingerprint": f"{service_name}:missing",
                        "message": self._format_missing(service_name),
                    }
                )
                continue
            container.reload()
            state = container.attrs.get("State", {})
            status = state.get("Status", "unknown")
            health = state.get("Health", {}).get("Status")
            failing = status != "running" or (health and health != "healthy")
            if not failing:
                continue
            fingerprint = f"{service_name}:{status}:{health}:{state.get('FinishedAt')}:{state.get('ExitCode')}"
            failures.append(
                {
                    "service": service_name,
                    "fingerprint": fingerprint,
                    "message": self._format_container_failure(service_name, container, state, health),
                }
            )
        return failures

    def _containers_by_service(self) -> dict[str, docker.models.containers.Container]:
        containers = self.client.containers.list(all=True, filters={"label": f"com.docker.compose.project={self.project_name}"})
        result: dict[str, docker.models.containers.Container] = {}
        for container in containers:
            service_name = container.labels.get("com.docker.compose.service")
            if service_name:
                result[service_name] = container
        return result

    def _format_missing(self, service_name: str) -> str:
        return "\n".join(
            [
                "❌ CLUSTER SERVICE FAILED",
                f"service: {service_name}",
                "status: ❌ missing",
                "details: container not found in the compose project",
            ]
        )

    def _format_container_failure(
        self,
        service_name: str,
        container: docker.models.containers.Container,
        state: dict[str, Any],
        health: str | None,
    ) -> str:
        lines = [
            "❌ CLUSTER SERVICE FAILED",
            f"service: {service_name}",
            f"container: {container.name}",
            f"status: {emoji_for(state.get('Status'))} {state.get('Status')}",
            f"health: {emoji_for(health, 'ℹ️')} {health or 'n/a'}",
            f"exit_code: {state.get('ExitCode')}",
            f"oom_killed: {state.get('OOMKilled')}",
            f"error: {state.get('Error') or '-'}",
            f"started_at: {state.get('StartedAt')}",
            f"finished_at: {state.get('FinishedAt')}",
        ]
        return "\n".join(lines)


def configure_logging() -> None:
    level = os.getenv("ALERT_BOT_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


def format_exception_message(title: str, details: str) -> str:
    return "\n".join([title, f"details: {details}"])


def format_recovery_message(title: str, details: list[str]) -> str:
    return "\n".join([title, *details])


def emoji_for(value: str | None, default: str = "ℹ️") -> str:
    if not value:
        return default
    return STATE_EMOJI.get(value.lower(), default)


def main() -> int:
    configure_logging()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    poll_interval = env_int("ALERT_BOT_POLL_INTERVAL_SECONDS", 30)
    request_timeout = env_int("ALERT_BOT_REQUEST_TIMEOUT_SECONDS", 15)
    max_log_lines = env_int("ALERT_BOT_MAX_LOG_LINES", 40)
    max_dags = env_int("ALERT_BOT_MAX_DAGS", 100)
    project_name = os.getenv("COMPOSE_PROJECT_NAME", "wather")
    monitored_services = env_csv(
        "ALERT_BOT_MONITORED_SERVICES",
        ["airflow-db", "airflow-scheduler", "airflow-web", "hive-metastore", "metastore-db", "minio", "trino"],
    )
    state = StateStore(Path(os.getenv("ALERT_BOT_STATE_FILE", "/state/alert_state.json")))
    alerter = TelegramAlerter(token=token, chat_id=chat_id, timeout=request_timeout)
    airflow = AirflowMonitor(
        base_url=os.getenv("AIRFLOW_BASE_URL", "http://airflow-web:8080"),
        username=os.getenv("AIRFLOW_USERNAME", "admin"),
        password=env_value("AIRFLOW_PASSWORD", "admin"),
        project_root=Path(os.getenv("ALERT_BOT_PROJECT_ROOT", "/opt/alert-bot/project")),
        max_dags=max_dags,
        max_log_lines=max_log_lines,
        request_timeout=request_timeout,
    )
    docker_monitor = DockerMonitor(project_name=project_name, monitored_services=monitored_services, max_log_lines=max_log_lines)

    LOG.info("Alert bot started on %s for compose project %s", hostname(), project_name)
    if not alerter.enabled():
        LOG.warning("Telegram credentials are not configured. Alerts will only be logged.")

    while True:
        dirty = False

        try:
            latest_runs = airflow.list_latest_runs()
            failed_runs = airflow.list_failed_runs()
            seen_services = state.data.setdefault("services", {})
            if "airflow-api" in seen_services:
                message = format_recovery_message(
                    "✅ AIRFLOW API RECOVERED",
                    ["details: Airflow API is reachable again"],
                )
                LOG.info(message)
                alerter.send(message)
                del seen_services["airflow-api"]
                dirty = True
            dag_status = state.data.setdefault("dag_status", {})
            for item in latest_runs:
                dag_id = item["dag_id"]
                run = item["run"]
                current_state = run.get("state")
                current_run_id = run.get("dag_run_id")
                previous = dag_status.get(dag_id)
                if (
                    current_state == "success"
                    and previous
                    and previous.get("state") == "failed"
                ):
                    message = format_recovery_message(
                        "✅ AIRFLOW DAG RECOVERED",
                        [
                            f"dag_id: {dag_id}",
                            f"failed_run_id: {previous.get('run_id')}",
                            f"recovered_run_id: {current_run_id}",
                            "state: ✅ success",
                            f"end_date: {run.get('end_date')}",
                        ],
                    )
                    LOG.info(message)
                    alerter.send(message)
                dag_status[dag_id] = {
                    "state": current_state,
                    "run_id": current_run_id,
                    "end_date": run.get("end_date"),
                }
                dirty = True

            seen_runs = state.data.setdefault("dag_failures", {})
            for item in failed_runs:
                dag_id = item["dag_id"]
                run = item["run"]
                key = f"{dag_id}:{run['dag_run_id']}"
                fingerprint = f"{run.get('state')}:{run.get('end_date')}"
                if seen_runs.get(key) == fingerprint:
                    continue
                message = airflow.build_failure_message(dag_id, run)
                LOG.error(message)
                alerter.send(message)
                seen_runs[key] = fingerprint
                dirty = True
        except Exception as exc:
            seen_services = state.data.setdefault("services", {})
            fingerprint = str(exc)
            if seen_services.get("airflow-api") != fingerprint:
                message = format_exception_message("❌ AIRFLOW API UNAVAILABLE", str(exc))
                LOG.error(message)
                alerter.send(message)
                seen_services["airflow-api"] = fingerprint
                dirty = True

        try:
            service_failures = docker_monitor.collect_failures()
            seen_services = state.data.setdefault("services", {})
            active_service_names = set()
            for failure in service_failures:
                service_name = failure["service"]
                active_service_names.add(service_name)
                fingerprint = failure["fingerprint"]
                if seen_services.get(service_name) == fingerprint:
                    continue
                LOG.error(failure["message"])
                alerter.send(failure["message"])
                seen_services[service_name] = fingerprint
                dirty = True
            for service_name in list(seen_services):
                if service_name in docker_monitor.monitored_services and service_name not in active_service_names:
                    message = format_recovery_message(
                        "✅ CLUSTER SERVICE RECOVERED",
                        [
                            f"service: {service_name}",
                            "status: 🟡 running",
                            "details: service health returned to normal",
                        ],
                    )
                    LOG.info(message)
                    alerter.send(message)
                    del seen_services[service_name]
                    dirty = True
        except Exception:
            LOG.exception("Docker monitoring iteration failed")

        if dirty:
            state.save()
        time.sleep(poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
