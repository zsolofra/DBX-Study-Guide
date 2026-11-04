from datetime import date

import pytest

D1, D2, D3 = date(2026, 1, 1), date(2026, 3, 1), date(2026, 6, 1)


def _initial(scd):
    return scd.scd2_merge([], [
        {"customer_id": 1, "region": "NA", "segment": "smb"},
        {"customer_id": 2, "region": "EMEA", "segment": "consumer"},
    ], key="customer_id", tracked=["region", "segment"], as_of=D1)


def test_scd1_upsert(scd):
    dim = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    out = scd.scd1_upsert(dim, [{"id": 2, "name": "B"}, {"id": 3, "name": "c"}], key="id")
    assert out == [{"id": 1, "name": "a"}, {"id": 2, "name": "B"}, {"id": 3, "name": "c"}]
    assert dim[1]["name"] == "b", "input must not be mutated"


def test_scd2_initial_load(scd):
    dim = _initial(scd)
    assert [r["sk"] for r in dim] == [1, 2]
    assert all(r["is_current"] and r["valid_to"] is None and r["valid_from"] == D1 for r in dim)


def test_scd2_change_closes_and_inserts(scd):
    dim = _initial(scd)
    out = scd.scd2_merge(dim, [{"customer_id": 1, "region": "APAC", "segment": "smb"}],
                         key="customer_id", tracked=["region", "segment"], as_of=D2)
    c1 = [r for r in out if r["customer_id"] == 1]
    assert len(c1) == 2
    old, new = sorted(c1, key=lambda r: r["sk"])
    assert (old["is_current"], old["valid_to"]) == (False, D2)
    assert (new["is_current"], new["valid_from"], new["region"], new["sk"]) == (True, D2, "APAC", 3)
    assert dim[0]["is_current"] is True, "input must not be mutated"


def test_scd2_is_idempotent(scd):
    dim = _initial(scd)
    same = [{"customer_id": 1, "region": "NA", "segment": "smb"}]
    assert scd.scd2_merge(dim, same, key="customer_id", tracked=["region", "segment"], as_of=D2) == dim


def test_scd2_new_key_and_untouched_keys(scd):
    dim = _initial(scd)
    out = scd.scd2_merge(dim, [{"customer_id": 3, "region": "LATAM", "segment": "enterprise"}],
                         key="customer_id", tracked=["region", "segment"], as_of=D2)
    assert len(out) == 3
    assert sum(r["is_current"] for r in out) == 3


def test_scd2_rejects_duplicate_source_keys(scd):
    with pytest.raises(ValueError):
        scd.scd2_merge([], [{"customer_id": 1, "region": "NA"}, {"customer_id": 1, "region": "EMEA"}],
                       key="customer_id", tracked=["region"], as_of=D1)


def test_point_in_time_sk(scd):
    dim = _initial(scd)
    dim = scd.scd2_merge(dim, [{"customer_id": 1, "region": "APAC", "segment": "smb"}],
                         key="customer_id", tracked=["region", "segment"], as_of=D2)
    dim = scd.scd2_merge(dim, [{"customer_id": 1, "region": "LATAM", "segment": "smb"}],
                         key="customer_id", tracked=["region", "segment"], as_of=D3)
    assert scd.point_in_time_sk(dim, "customer_id", 1, date(2026, 2, 1)) == 1
    assert scd.point_in_time_sk(dim, "customer_id", 1, D2) == 3          # valid_from inclusive
    assert scd.point_in_time_sk(dim, "customer_id", 1, date(2026, 12, 1)) == 4
    assert scd.point_in_time_sk(dim, "customer_id", 1, date(2025, 1, 1)) is None
    assert scd.point_in_time_sk(dim, "customer_id", 99, D2) is None
