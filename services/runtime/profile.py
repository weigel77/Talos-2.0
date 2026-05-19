"""Runtime profile selection for local and future hosted Delphi deployments."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

from flask import Flask

from config import AppConfig, HOSTED_RUNTIME_BASE_URL, HOSTED_RUNTIME_HOST, HOSTED_RUNTIME_PORT, is_render_production_environment


@dataclass(frozen=True)
class RuntimeProfile:
    """Resolved runtime profile settings for the current process."""

    profile_name: str
    host: str
    port: int
    use_https: bool
    ssl_context: tuple[str, str] | None
    launch_url: str
    auto_open_browser: bool
    browser_launch_delay_seconds: float
    auto_start_background_services: bool
    register_shutdown_hooks: bool
    testing: bool


def select_runtime_profile(app: Flask, config: AppConfig) -> RuntimeProfile:
    """Resolve the currently active runtime profile from Flask and app config."""
    runtime_target = str(app.config.get("RUNTIME_TARGET") or config.runtime_target or "local").strip().lower() or "local"
    testing = bool(app.config.get("TESTING"))

    if runtime_target == "hosted":
        bind_host = str(app.config.get("APP_HOST") or config.app_host or "0.0.0.0").strip() or "0.0.0.0"
        launch_url = str(app.config.get("HOSTED_PUBLIC_BASE_URL") or config.hosted_public_base_url or "").strip()
        render_production = is_render_production_environment(
            hosted_public_base_url=launch_url,
            deployment_env=str(app.config.get("DELPHI_DEPLOYMENT_ENV") or os.getenv("DELPHI_DEPLOYMENT_ENV") or ""),
        )
        if render_production:
            bind_port = int(os.environ.get("PORT", HOSTED_RUNTIME_PORT))
        else:
            bind_port = int(app.config.get("APP_PORT") or config.app_port or HOSTED_RUNTIME_PORT)
        if not launch_url:
            if testing:
                launch_url = HOSTED_RUNTIME_BASE_URL
            else:
                raise RuntimeError("Hosted Delphi 6.0 requires HOSTED_PUBLIC_BASE_URL so browser clients do not depend on localhost assumptions.")
        parsed = urlparse(launch_url)
        host = bind_host
        port = bind_port
        use_https = parsed.scheme == "https" and not render_production
        ssl_context = None
        if use_https and (parsed.hostname or "").strip().lower() in {"127.0.0.1", "localhost"}:
            ssl_context = ("localhost+2.pem", "localhost+2-key.pem")
        return RuntimeProfile(
            profile_name="hosted-testing" if testing else "hosted-server",
            host=host,
            port=port,
            use_https=use_https,
            ssl_context=ssl_context,
            launch_url=launch_url,
            auto_open_browser=False,
            browser_launch_delay_seconds=0.0,
            auto_start_background_services=not testing,
            register_shutdown_hooks=not testing,
            testing=testing,
        )

    use_https = bool(config.market_data_provider == "schwab" and config.schwab_redirect_uri.startswith("https://"))
    if use_https:
        parsed = urlparse(config.schwab_redirect_uri)
        host = parsed.hostname or config.app_host
        port = parsed.port or config.app_port
        launch_url = f"https://{host}:{port}"
    else:
        host = config.app_host
        port = config.app_port
        launch_url = f"http://localhost:{port}"

    return RuntimeProfile(
        profile_name="local-testing" if testing else "local-desktop",
        host=host,
        port=port,
        use_https=use_https,
        ssl_context=("localhost+2.pem", "localhost+2-key.pem") if use_https else None,
        launch_url=launch_url,
        auto_open_browser=not testing,
        browser_launch_delay_seconds=1.0,
        auto_start_background_services=not testing,
        register_shutdown_hooks=not testing,
        testing=testing,
    )