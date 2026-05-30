"""Parser for the SMN EMA station registry (fixed-width TXT)."""

import logging
import re
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StationMetadata:
    """One station from the SMN registry, with coordinates in decimal degrees."""

    station_id: int
    name: str
    province: str
    latitude: float
    longitude: float
    altitude_meters: int
    oaci_code: Optional[str]


# The TXT is fixed-width with two header lines followed by data rows. The NOMBRE
# column is exactly 30 chars wide; longer names overflow onto a continuation
# line that has data only in columns 0..29 and whitespace beyond. The rest of
# the row uses runs-of-spaces between fields, which we capture with this regex.
_ROW_TAIL_RE = re.compile(
    r"^\s*"
    r"(?P<province>.+?)"
    r"\s{2,}(?P<lat_deg>-?\d+)\s+(?P<lat_min>\d+)"
    r"\s+(?P<lon_deg>-?\d+)\s+(?P<lon_min>\d+)"
    r"\s+(?P<altura>-?\d+)\s+(?P<nro>\d+)"
    r"(?:\s+(?P<oaci>[A-Z0-9]+))?"
    r"\s*$"
)
_NAME_COLUMN_WIDTH = 30


def _dms_to_decimal(deg: int, minutes: int) -> float:
    """Convert signed degrees + unsigned minutes to a signed decimal degree."""
    sign = -1 if deg < 0 else 1
    return deg + sign * (minutes / 60.0)


def parse_estaciones_txt(content: str) -> List[StationMetadata]:
    """
    Parse the unzipped `estaciones.txt` body into station metadata records.

    Skips the two header lines, merges continuation lines (longer-than-30-char
    names), and tolerates rows it can't recognize by logging + skipping rather
    than blowing up the whole parse.
    """
    raw_lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    # Header is 2 lines (column titles + unit annotations).
    data_lines = raw_lines[2:]

    parsed: List[StationMetadata] = []
    for raw_line in data_lines:
        if not raw_line.strip():
            continue

        padded = raw_line if len(raw_line) >= _NAME_COLUMN_WIDTH else raw_line.ljust(
            _NAME_COLUMN_WIDTH
        )
        tail = padded[_NAME_COLUMN_WIDTH:]

        if not tail.strip():
            # Continuation line: append the leading chars to the previous name.
            suffix = padded[:_NAME_COLUMN_WIDTH].rstrip()
            if not parsed or not suffix:
                continue
            last = parsed[-1]
            parsed[-1] = StationMetadata(
                station_id=last.station_id,
                name=(last.name + suffix).strip(),
                province=last.province,
                latitude=last.latitude,
                longitude=last.longitude,
                altitude_meters=last.altitude_meters,
                oaci_code=last.oaci_code,
            )
            continue

        match = _ROW_TAIL_RE.match(tail)
        if not match:
            logger.warning(
                "Could not parse SMN registry row: %r", raw_line[:80]
            )
            continue

        try:
            parsed.append(
                StationMetadata(
                    station_id=int(match.group("nro")),
                    name=padded[:_NAME_COLUMN_WIDTH].strip(),
                    province=match.group("province").strip(),
                    latitude=_dms_to_decimal(
                        int(match.group("lat_deg")), int(match.group("lat_min"))
                    ),
                    longitude=_dms_to_decimal(
                        int(match.group("lon_deg")), int(match.group("lon_min"))
                    ),
                    altitude_meters=int(match.group("altura")),
                    oaci_code=match.group("oaci"),
                )
            )
        except (TypeError, ValueError) as exc:
            logger.warning(
                "Could not convert SMN registry row fields (%s): %r",
                exc,
                raw_line[:80],
            )

    return parsed


def station_metadata_to_jsonable(stations: List[StationMetadata]) -> List[dict]:
    """Render a list of `StationMetadata` as plain dicts suitable for JSON."""
    return [
        {
            "station_id": s.station_id,
            "name": s.name,
            "province": s.province,
            "latitude": s.latitude,
            "longitude": s.longitude,
            "altitude_meters": s.altitude_meters,
            "oaci_code": s.oaci_code,
        }
        for s in stations
    ]
