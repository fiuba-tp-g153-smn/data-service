"""Structural guards on settings.json, derived from the code rather than a list.

The loader warns on keys it doesn't recognize instead of failing, so a rename
whose read site was missed leaves the service silently on a default. These
guards close that gap by checking the file against what `settings.py` declares
and what the rest of `src/` actually reads.
"""

import ast
import json
from pathlib import Path

from settings import Settings

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SRC = _REPO_ROOT / "src"


def _settings_json_keys() -> dict:
    """The live settings.json, flattened the way the loader flattens it."""
    data = json.loads((_REPO_ROOT / "settings.json").read_text(encoding="utf-8"))
    return Settings._flatten(data)  # pylint: disable=protected-access


def _attributes_read_outside_settings_module() -> set[str]:
    """Every attribute name accessed anywhere in src/, except in settings.py.

    Collected from the AST rather than by grepping the text, so a key that
    survives only as a mention in a docstring or comment does not count as a
    read — that is precisely the half-finished rename this guard looks for.

    settings.py itself is excluded because it declares and env-loads every key,
    which would make the check pass for a key nothing else consumes.
    """
    names: set[str] = set()
    for path in sorted(_SRC.rglob("*.py")):
        if path.name == "settings.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                names.add(node.attr)
    return names


def test_every_settings_json_key_is_recognized_by_the_loader():
    """A key outside `_JSON_KEYS` is dropped with a warning, not applied."""
    unknown = sorted(
        set(_settings_json_keys())
        - Settings._JSON_KEYS  # pylint: disable=protected-access
    )
    assert not unknown, (
        f"settings.json keys the loader would ignore: {unknown}. "
        "Add them to Settings._JSON_KEYS or fix the spelling."
    )


def test_every_settings_json_key_is_read_somewhere_in_src():
    """A tuned key no code reads is a rename that lost its read site."""
    read = _attributes_read_outside_settings_module()
    unread = sorted(key for key in _settings_json_keys() if key not in read)
    assert not unread, (
        f"settings.json tunes keys nothing in src/ reads: {unread}. "
        "Either a read site still uses the pre-rename attribute name, or the "
        "key is dead and should come out of settings.json."
    )
