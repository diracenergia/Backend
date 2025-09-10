# app/repos/presence.py
from typing import Optional

def bump_presence(asset_type: str, asset_id: str, ts: Optional[float] = None) -> None:
    # No-op temporal para evitar fallos si el módulo real no está implementado.
    return None
