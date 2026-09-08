#!/usr/bin/env python3
"""bian production knowledge loop. Vault is required; it never submits orders."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

PROJECT_VAULT = Path("/mnt/d/obisidian/Obsidian/Project")
BIAN_ROOT = PROJECT_VAULT / "虚拟币" / "bian"
DAILY_DIR = BIAN_ROOT / "daily"
STATUS_PATH = BIAN_ROOT / "状态.md"


def require_production_vaults() -> dict:
    missing = []
    for label, path in (
        ("project_vault", PROJECT_VAULT),
        ("bian_root", BIAN_ROOT),
        ("daily_dir", DAILY_DIR),
        ("status", STATUS_PATH),
    ):
        if not path.exists():
            missing.append(f"{label}={path}")
    if missing:
        raise RuntimeError("OBSIDIAN_VAULT_UNAVAILABLE: " + "; ".join(missing))
    return {
        "project_vault": str(PROJECT_VAULT),
        "daily_dir": str(DAILY_DIR),
        "status": str(STATUS_PATH),
        "daily_folder": "虚拟币/bian/daily",
    }


def write_daily_note() -> Path:
    require_production_vaults()
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).date().isoformat()
    path = DAILY_DIR / f"{today}.md"
    if not path.exists():
        path.write_text(
            "\n".join([
                f"# {today}",
                "",
                "## bian knowledge loop",
                "",
                "- Market: Binance USD-M Futures",
                "- Universe: BTCUSDT / ETHUSDT / BNBUSDT",
                "- Vault: `虚拟币/bian/daily`",
                "- Execution owner remains `execution.py`; this note does not submit orders.",
                "",
            ]),
            encoding="utf-8",
        )
    return path


def main() -> None:
    vaults = require_production_vaults()
    note = write_daily_note()
    print("bian knowledge loop OK", vaults["daily_dir"], note)


if __name__ == "__main__":
    main()
