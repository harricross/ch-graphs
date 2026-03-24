"""Tests for vis.py — pure-Python graph-building helpers."""

import pytest
from vis import _build_vis_data, _compute_positions


# ---------------------------------------------------------------------------
# Helpers to build minimal node/rel dicts that _build_vis_data expects
# ---------------------------------------------------------------------------

def _node(nid, label, props=None):
    return {"id": nid, "labels": [label], "properties": props or {}}


def _rel(start, end, rel_type, props=None):
    return {"startId": start, "endId": end, "type": rel_type, "properties": props or {}}


def _nodes(*nodes):
    return {n["id"]: n for n in nodes}


# ---------------------------------------------------------------------------
# _build_vis_data
# ---------------------------------------------------------------------------

class TestBuildVisData:
    def test_empty_graph(self):
        result = _build_vis_data({}, [])
        assert result["nodes"] == []
        assert result["edges"] == []

    def test_single_company_node(self):
        nodes = _nodes(_node("c1", "Company", {"companyNumber": "00000001", "name": "Test Co"}))
        result = _build_vis_data(nodes, [])
        assert len(result["nodes"]) == 1
        assert result["nodes"][0]["group"] == "Company"

    def test_company_label_includes_company_number(self):
        nodes = _nodes(_node("c1", "Company", {"companyNumber": "00000001", "name": "Test Co"}))
        result = _build_vis_data(nodes, [])
        label = result["nodes"][0]["label"]
        assert "00000001" in label

    def test_long_name_truncated(self):
        long_name = "A" * 50
        nodes = _nodes(_node("c1", "Company", {"name": long_name, "companyNumber": "00000001"}))
        result = _build_vis_data(nodes, [])
        label = result["nodes"][0]["label"]
        assert "..." in label

    def test_corporate_entity_merged_into_company(self):
        """CorporateEntity linked via IS_COMPANY should be merged into the Company node."""
        nodes = _nodes(
            _node("ce1", "CorporateEntity", {"name": "Corp Ltd"}),
            _node("c1", "Company", {"companyNumber": "00000001", "name": "Corp Ltd"}),
        )
        rels = [_rel("ce1", "c1", "IS_COMPANY")]
        result = _build_vis_data(nodes, rels)
        groups = {n["group"] for n in result["nodes"]}
        # CorporateEntity should be merged away — only Company remains
        assert "CorporateEntity" not in groups
        assert "Company" in groups

    def test_ceased_psc_edge_filtered_out(self):
        """Edges with ceasedOn set should be excluded from vis edges."""
        nodes = _nodes(
            _node("p1", "Person", {"name": "Alice Smith"}),
            _node("c1", "Company", {"companyNumber": "00000001", "name": "Test Co"}),
        )
        rels = [_rel("p1", "c1", "HAS_SIGNIFICANT_CONTROL", {"ceasedOn": "2020-01-01"})]
        result = _build_vis_data(nodes, rels)
        assert result["edges"] == []

    def test_active_psc_edge_kept(self):
        """Edges without ceasedOn should be present."""
        nodes = _nodes(
            _node("p1", "Person", {"name": "Bob Jones"}),
            _node("c1", "Company", {"companyNumber": "00000001", "name": "Test Co"}),
        )
        rels = [_rel("p1", "c1", "HAS_SIGNIFICANT_CONTROL", {})]
        result = _build_vis_data(nodes, rels)
        assert len(result["edges"]) == 1

    def test_duplicate_edges_deduplicated(self):
        nodes = _nodes(
            _node("p1", "Person", {"name": "Alice"}),
            _node("c1", "Company", {"companyNumber": "00000001", "name": "Co"}),
        )
        rels = [
            _rel("p1", "c1", "HAS_SIGNIFICANT_CONTROL"),
            _rel("p1", "c1", "HAS_SIGNIFICANT_CONTROL"),
        ]
        result = _build_vis_data(nodes, rels)
        assert len(result["edges"]) == 1

    def test_node_colors_assigned_by_label(self):
        nodes = _nodes(
            _node("c1", "Company", {"companyNumber": "00000001", "name": "Co"}),
            _node("p1", "Person", {"name": "Alice"}),
        )
        rels = [_rel("p1", "c1", "HAS_SIGNIFICANT_CONTROL")]
        result = _build_vis_data(nodes, rels)
        colors = {n["group"]: n["color"] for n in result["nodes"]}
        # Company and Person should have different colours
        assert colors.get("Company") != colors.get("Person")

    def test_officer_of_edge_present(self):
        nodes = _nodes(
            _node("d1", "Director", {"name": "SMITH, JOHN"}),
            _node("c1", "Company", {"companyNumber": "00000001", "name": "Co"}),
        )
        rels = [_rel("d1", "c1", "OFFICER_OF", {"role": "director"})]
        result = _build_vis_data(nodes, rels)
        assert len(result["edges"]) == 1

    def test_orphaned_non_company_nodes_removed(self):
        """Nodes with no remaining edges (after filters) should be dropped, except Company nodes."""
        nodes = _nodes(
            _node("p1", "Person", {"name": "Orphan"}),
            _node("c1", "Company", {"companyNumber": "00000001", "name": "Co"}),
        )
        # Person has no edges; company stands alone
        result = _build_vis_data(nodes, [])
        groups = [n["group"] for n in result["nodes"]]
        assert "Person" not in groups
        assert "Company" in groups

    def test_result_has_nodes_and_edges_keys(self):
        result = _build_vis_data({}, [])
        assert "nodes" in result
        assert "edges" in result

    def test_person_director_merged_when_names_match(self):
        """A Director with the same normalised name as a Person should be merged."""
        nodes = _nodes(
            _node("p1", "Person", {"surname": "SMITH", "forename": "JOHN"}),
            _node("d1", "Director", {"name": "SMITH, JOHN"}),
            _node("c1", "Company", {"companyNumber": "00000001", "name": "Co"}),
        )
        rels = [
            _rel("p1", "c1", "HAS_SIGNIFICANT_CONTROL"),
            _rel("d1", "c1", "OFFICER_OF", {"role": "director"}),
        ]
        result = _build_vis_data(nodes, rels)
        groups = [n["group"] for n in result["nodes"]]
        # Director should be merged into Person
        assert "Director" not in groups


# ---------------------------------------------------------------------------
# _compute_positions
# ---------------------------------------------------------------------------

class TestComputePositions:
    def _make_vis_node(self, nid, group, level=0):
        return {"id": nid, "group": group, "level": level}

    def _make_vis_edge(self, src, dst):
        return {"from": src, "to": dst}

    def test_empty_graph_no_crash(self):
        _compute_positions([], [])  # should not raise

    def test_company_nodes_get_positions(self):
        nodes = [self._make_vis_node("c1", "Company", 0)]
        _compute_positions(nodes, [])
        assert "x" in nodes[0]
        assert "y" in nodes[0]

    def test_satellite_nodes_get_positions(self):
        company = self._make_vis_node("c1", "Company", 0)
        person = self._make_vis_node("p1", "Person", 1)
        edges = [self._make_vis_edge("p1", "c1")]
        _compute_positions([company, person], edges)
        assert "x" in person
        assert "y" in person

    def test_multiple_companies_at_same_level(self):
        nodes = [
            self._make_vis_node("c1", "Company", 0),
            self._make_vis_node("c2", "Company", 0),
        ]
        _compute_positions(nodes, [])
        # Both should get placed
        assert "x" in nodes[0] and "x" in nodes[1]
        # They should not share the same x coordinate
        assert nodes[0]["x"] != nodes[1]["x"]

    def test_companies_at_different_levels_have_different_y(self):
        nodes = [
            self._make_vis_node("c1", "Company", 0),
            self._make_vis_node("c2", "Company", 1),
        ]
        _compute_positions(nodes, [])
        assert nodes[0]["y"] != nodes[1]["y"]

    def test_orphan_nodes_still_placed(self):
        """Non-company nodes with no edges should still receive coordinates."""
        company = self._make_vis_node("c1", "Company", 0)
        orphan = self._make_vis_node("p1", "Person", 0)
        # No edge between them
        _compute_positions([company, orphan], [])
        # Orphan should be placed somewhere
        assert "x" in orphan
        assert "y" in orphan
