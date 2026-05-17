"""Unit tests for the SMN EMA registry parser."""

from services.smn_stations_registry import (
    StationMetadata,
    parse_estaciones_txt,
    station_metadata_to_jsonable,
)

_HEADER = (
    "NOMBRE                         PROVINCIA                              "
    "LATITUD          LONGITUD       ALTURA  NRO   NroOACI\n"
    "                                                                     "
    "[gr]    [min]    [gr]    [min]       [m]\n"
)


def _row(*lines: str) -> str:
    return _HEADER + "\n".join(lines) + "\n"


def test_parses_a_simple_row():
    txt = _row(
        "BASE BELGRANO II               ANTARTIDA                            "
        "-77      52       -34      37        256  89034 SAYB"
    )
    stations = parse_estaciones_txt(txt)
    assert len(stations) == 1
    s = stations[0]
    assert s.station_id == 89034
    assert s.name == "BASE BELGRANO II"
    assert s.province == "ANTARTIDA"
    assert s.altitude_meters == 256
    assert s.oaci_code == "SAYB"
    # -77°52' → -77 + (-1)*52/60 = -77.8666…
    assert abs(s.latitude - (-77.8667)) < 0.001
    assert abs(s.longitude - (-34.6167)) < 0.001


def test_merges_continuation_lines_into_name():
    txt = _row(
        "PRESIDENCIA ROQUE SAENZ PEÑA A CHACO                                "
        "-26      44       -60      29         93  87148 SARS",
        "ERO",
    )
    stations = parse_estaciones_txt(txt)
    assert len(stations) == 1
    assert stations[0].name == "PRESIDENCIA ROQUE SAENZ PEÑA AERO"
    assert stations[0].province == "CHACO"
    assert stations[0].station_id == 87148


def test_handles_multi_word_province():
    txt = _row(
        "USHUAIA AERO                   TIERRA DEL FUEGO                     "
        "-54      50       -68      18         57  87938 SAWH"
    )
    stations = parse_estaciones_txt(txt)
    assert len(stations) == 1
    assert stations[0].province == "TIERRA DEL FUEGO"
    assert stations[0].station_id == 87938


def test_handles_missing_oaci():
    txt = _row(
        "JUJUY U N                      JUJUY                                "
        "-24      10       -65      19       1302  87043"
    )
    stations = parse_estaciones_txt(txt)
    assert len(stations) == 1
    assert stations[0].oaci_code is None
    assert stations[0].station_id == 87043


def test_skips_blank_and_unparseable_rows_without_failing():
    txt = _row(
        "BASE BELGRANO II               ANTARTIDA                            "
        "-77      52       -34      37        256  89034 SAYB",
        "",
        "garbage line that doesn't match anything at all",
        "USHUAIA AERO                   TIERRA DEL FUEGO                     "
        "-54      50       -68      18         57  87938 SAWH",
    )
    stations = parse_estaciones_txt(txt)
    ids = [s.station_id for s in stations]
    assert 89034 in ids
    assert 87938 in ids
    # garbage line shouldn't appear; we tolerate it.
    assert len(stations) == 2


def test_continuation_with_no_prior_row_is_ignored():
    txt = _HEADER + "ERO\n"
    assert parse_estaciones_txt(txt) == []


def test_jsonable_output_shape():
    s = StationMetadata(
        station_id=1,
        name="X",
        province="P",
        latitude=-30.0,
        longitude=-60.0,
        altitude_meters=42,
        oaci_code="ABCD",
    )
    [out] = station_metadata_to_jsonable([s])
    assert out == {
        "station_id": 1,
        "name": "X",
        "province": "P",
        "latitude": -30.0,
        "longitude": -60.0,
        "altitude_meters": 42,
        "oaci_code": "ABCD",
    }
