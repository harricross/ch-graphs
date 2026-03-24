"""Tests for search.py — pure-Python helpers that need no Neo4j connection."""

import pytest
from search import (
    _compute_levels,
    company_name_query,
    directors_for_tree_query,
    make_director_id,
    ownership_tree_query,
    person_query,
)


# ---------------------------------------------------------------------------
# make_director_id
# ---------------------------------------------------------------------------

class TestMakeDirectorId:
    def test_uses_self_link_when_present(self):
        officer = {"links": {"self": "/officers/abc123"}, "name": "SMITH, JOHN"}
        assert make_director_id(officer) == "/officers/abc123"

    def test_falls_back_to_name_and_dob(self):
        officer = {"name": "Smith, John", "date_of_birth": {"month": 3, "year": 1975}}
        result = make_director_id(officer)
        assert result == "SMITH, JOHN|3|1975"

    def test_empty_links_dict_falls_back(self):
        officer = {"links": {}, "name": "Jones, Alice", "date_of_birth": {"month": 7, "year": 1980}}
        result = make_director_id(officer)
        assert result == "JONES, ALICE|7|1980"

    def test_missing_dob_produces_empty_fields(self):
        officer = {"name": "Brown, Bob"}
        result = make_director_id(officer)
        assert result == "BROWN, BOB||"

    def test_result_is_uppercase(self):
        officer = {"name": "lower case", "date_of_birth": {"month": 1, "year": 2000}}
        result = make_director_id(officer)
        assert result == result.upper()


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------

class TestOwnershipTreeQuery:
    def test_contains_company_number(self):
        q = ownership_tree_query("00253240", 10)
        assert "00253240" in q

    def test_contains_depth(self):
        q = ownership_tree_query("00253240", 15)
        assert "15" in q

    def test_returns_string(self):
        assert isinstance(ownership_tree_query("12345678", 5), str)


class TestDirectorsForTreeQuery:
    def test_contains_company_number(self):
        q = directors_for_tree_query("00253240", 10)
        assert "00253240" in q

    def test_contains_depth(self):
        q = directors_for_tree_query("00253240", 20)
        assert "20" in q

    def test_contains_officer_of(self):
        q = directors_for_tree_query("00253240", 5)
        assert "OFFICER_OF" in q


class TestCompanyNameQuery:
    def test_contains_limit(self):
        q = company_name_query("tesco", 25)
        assert "25" in q

    def test_uses_name_parameter(self):
        q = company_name_query("barclays", 10)
        assert "$name" in q


class TestPersonQuery:
    def test_contains_limit(self):
        q = person_query("Smith", 30)
        assert "30" in q

    def test_uses_name_parameter(self):
        q = person_query("Jones", 5)
        assert "$name" in q


# ---------------------------------------------------------------------------
# _compute_levels
# ---------------------------------------------------------------------------

def _make_node(nid, label="Company"):
    return {nid: {"id": nid, "labels": [label], "properties": {}}}


def _make_rel(start, end, rel_type="HAS_SIGNIFICANT_CONTROL"):
    return {"startId": start, "endId": end, "type": rel_type, "properties": {}}


class TestComputeLevels:
    def test_empty_graph_returns_empty(self):
        assert _compute_levels({}, []) == {}

    def test_no_edges_returns_empty(self):
        nodes = {**_make_node("a"), **_make_node("b")}
        assert _compute_levels(nodes, []) == {}

    def test_single_edge_root_at_zero(self):
        # parent -> child: child is root (most incoming), parent is level 0
        nodes = {**_make_node("parent"), **_make_node("child")}
        rels = [_make_rel("parent", "child")]
        levels = _compute_levels(nodes, rels)
        # root (child, most incoming) should be at level 1 after normalisation
        assert levels["parent"] == 0
        assert levels["child"] == 1

    def test_chain_of_three(self):
        # grandparent -> parent -> child
        nodes = {**_make_node("gp"), **_make_node("p"), **_make_node("c")}
        rels = [_make_rel("gp", "p"), _make_rel("p", "c")]
        levels = _compute_levels(nodes, rels)
        assert levels["gp"] < levels["p"] < levels["c"]
        assert levels["gp"] == 0

    def test_normalised_min_is_zero(self):
        nodes = {**_make_node("a"), **_make_node("b"), **_make_node("c")}
        rels = [_make_rel("a", "b"), _make_rel("b", "c")]
        levels = _compute_levels(nodes, rels)
        assert min(levels.values()) == 0

    def test_root_hint_overrides_inference(self):
        # parent -> child, but we hint that parent is the root
        nodes = {**_make_node("parent"), **_make_node("child")}
        rels = [_make_rel("parent", "child")]
        levels_hint = _compute_levels(nodes, rels, root_hint="parent")
        # with parent as root it controls child, so parent level < child level
        assert levels_hint["parent"] < levels_hint["child"]

    def test_officer_of_edge_included(self):
        nodes = {**_make_node("director", "Director"), **_make_node("company")}
        rels = [_make_rel("director", "company", "OFFICER_OF")]
        levels = _compute_levels(nodes, rels)
        assert "director" in levels or "company" in levels

    def test_disconnected_nodes_not_assigned_level(self):
        # One isolated node, one connected pair
        nodes = {**_make_node("a"), **_make_node("b"), **_make_node("isolated")}
        rels = [_make_rel("a", "b")]
        levels = _compute_levels(nodes, rels)
        assert "isolated" not in levels
