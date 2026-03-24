"""Tests for fetch_directors.py — pure-Python helpers."""

from fetch_directors import make_director_id


class TestMakeDirectorId:
    def test_uses_self_link_when_present(self):
        officer = {"links": {"self": "/officers/xyz789"}, "name": "JONES, ALICE"}
        assert make_director_id(officer) == "/officers/xyz789"

    def test_falls_back_to_name_and_dob(self):
        officer = {"name": "Taylor, Robert", "date_of_birth": {"month": 11, "year": 1968}}
        result = make_director_id(officer)
        assert result == "TAYLOR, ROBERT|11|1968"

    def test_empty_officer_dict(self):
        officer = {}
        result = make_director_id(officer)
        assert result == "||"

    def test_result_is_uppercase(self):
        officer = {"name": "mixed Case", "date_of_birth": {"month": 6, "year": 1990}}
        result = make_director_id(officer)
        assert result == result.upper()

    def test_self_link_beats_name_dob(self):
        """Even when both name+dob and self link are present, self link wins."""
        officer = {
            "links": {"self": "/officers/abc"},
            "name": "SMITH, JOHN",
            "date_of_birth": {"month": 1, "year": 2000},
        }
        assert make_director_id(officer) == "/officers/abc"

    def test_missing_dob_fields_produce_empty_segments(self):
        officer = {"name": "BROWN, BOB", "date_of_birth": {}}
        result = make_director_id(officer)
        assert result == "BROWN, BOB||"
