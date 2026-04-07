from __future__ import annotations

import sys
from pathlib import Path
from tkinter import PhotoImage


_ICON_CACHE: dict[str, PhotoImage] = {}


def _resource_root() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(getattr(sys, "_MEIPASS"))
    return Path(__file__).resolve().parent.parent


def get_icon_png_path() -> Path:
    return _resource_root() / "assets" / "apple_icon.png"


def get_icon_ico_path() -> Path:
    return _resource_root() / "assets" / "apple_icon.ico"


def apply_window_icon(window) -> None:
    png_path = get_icon_png_path()
    ico_path = get_icon_ico_path()

    if png_path.exists():
        cache_key = str(png_path)
        image = _ICON_CACHE.get(cache_key)
        if image is None:
            image = PhotoImage(file=str(png_path))
            _ICON_CACHE[cache_key] = image
        window.iconphoto(True, image)
        setattr(window, "_app_icon_image", image)

    if ico_path.exists():
        try:
            window.iconbitmap(default=str(ico_path))
        except Exception:
            pass
