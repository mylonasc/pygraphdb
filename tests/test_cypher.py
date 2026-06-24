import pytest

from pygraphdb.graphdb import Node
from pygraphdb.cypher import QueryResult, _split_top_level_args, parse

from .conftest import populate_typed_graph


def test_cypher_label_scan_uses_indexed_labels(graph_db):
    graph_db.put_nodes([
        Node(node_id="drug-1", labels=["Drug"], properties={"kind": "drug"}),
        Node(node_id="protein-1", labels=["Protein"], properties={"kind": "protein"}),
    ])

    result = graph_db.query("MATCH (n:Drug) RETURN n")

    assert result.columns == ("n",)
    assert [record["n"].get_id for record in result] == ["drug-1"]


def test_cypher_label_scan_filters_property_with_index_when_registered(graph_db):
    graph_db.put_nodes([
        Node(node_id="drug-1", labels=["Drug"], properties={"kind": "drug", "name": "Aspirin"}),
        Node(node_id="drug-2", labels=["Drug"], properties={"kind": "drug", "name": "Ibuprofen"}),
    ])
    graph_db.create_node_property_index("name")

    result = graph_db.query('MATCH (n:Drug {name: "Aspirin"}) RETURN n')

    assert [record["n"].get_id for record in result] == ["drug-1"]


def test_cypher_label_scan_filters_property_without_registered_index(graph_db):
    graph_db.put_nodes([
        Node(node_id="drug-1", labels=["Drug"], properties={"name": "Aspirin"}),
        Node(node_id="drug-2", labels=["Drug"], properties={"name": "Ibuprofen"}),
        Node(node_id="drug-3", labels=["Drug"], properties={}),
    ])

    result = graph_db.query('MATCH (n:Drug {name: "Aspirin"}) RETURN n')

    assert [record["n"].get_id for record in result] == ["drug-1"]


def test_cypher_label_scan_rejects_unbound_return_variable(graph_db):
    with pytest.raises(ValueError, match="unbound variable"):
        graph_db.query("MATCH (n:Drug) RETURN m")


def test_cypher_one_hop_typed_match_returns_bound_nodes(graph_db):
    populate_typed_graph(graph_db)

    result = graph_db.query('MATCH (d {id: "drug-1"})-[:drug-to-protein]->(p) RETURN d, p')

    assert result.columns == ("d", "p")
    assert {record["d"].get_id for record in result} == {"drug-1"}
    assert {record["p"].get_id for record in result} == {"protein-1", "protein-2"}


def test_cypher_anchored_match_returns_empty_for_missing_source(graph_db):
    result = graph_db.query('MATCH (d {id: "missing"})-[:drug-to-protein]->(p) RETURN d, p')

    assert result.columns == ("d", "p")
    assert result.records == []


def test_cypher_anchored_match_skips_missing_target_node(graph_db):
    graph_db.put_node(Node(node_id="drug-1"))
    graph_db.store.put_typed_adjacency(b"drug-1", b"missing-protein", "drug-to-protein", b"e1")

    result = graph_db.query('MATCH (d {id: "drug-1"})-[:drug-to-protein]->(p) RETURN d, p')

    assert result.records == []


def test_cypher_one_hop_typed_match_can_bind_relationship(graph_db):
    populate_typed_graph(graph_db)

    result = graph_db.query('MATCH (d {id: "drug-1"})-[r:drug-to-disease]->(x) RETURN d, r, x')

    assert result.columns == ("d", "r", "x")
    assert len(result) == 1
    assert result.records[0]["r"].get_id == "d1-disease"
    assert result.records[0]["x"].get_id == "disease-1"


def test_cypher_multi_hop_typed_match_returns_bound_nodes(graph_db):
    populate_typed_graph(graph_db)

    result = graph_db.query('MATCH (d {id: "drug-1"})-[:drug-to-protein]->(p)-[:protein-to-disease]->(x) RETURN d, p, x')

    assert result.columns == ("d", "p", "x")
    assert {record["d"].get_id for record in result} == {"drug-1"}
    assert {(record["p"].get_id, record["x"].get_id) for record in result} == {
        ("protein-1", "disease-1"),
        ("protein-1", "disease-2"),
        ("protein-2", "disease-3"),
    }


def test_cypher_multi_hop_typed_match_can_bind_relationships(graph_db):
    populate_typed_graph(graph_db)

    result = graph_db.query('MATCH (d {id: "drug-1"})-[r1:drug-to-protein]->(p)-[r2:protein-to-disease]->(x) RETURN r1, r2, x')

    assert result.columns == ("r1", "r2", "x")
    assert {(record["r1"].get_type, record["r2"].get_type, record["x"].get_id) for record in result} == {
        ("drug-to-protein", "protein-to-disease", "disease-1"),
        ("drug-to-protein", "protein-to-disease", "disease-2"),
        ("drug-to-protein", "protein-to-disease", "disease-3"),
    }


def test_cypher_sample_typed_paths_call_returns_paths(graph_db):
    populate_typed_graph(graph_db)

    result = graph_db.query(
        'CALL pg.sample_typed_paths(["drug-1"], '
        '[{"edge_type": "drug-to-protein", "direction": "out", "sample_size": 2}, '
        '{"edge_type": "protein-to-disease", "direction": "out", "sample_size": 1}]) '
        'YIELD path RETURN path'
    )

    assert result.columns == ("path",)
    assert result.records
    for record in result:
        sampled_path = record["path"]
        assert sampled_path["seed"] == b"drug-1"
        assert len(sampled_path["path"]) == 2
        assert sampled_path["path"][0]["edge_type"] == "drug-to-protein"
        assert sampled_path["path"][1]["edge_type"] == "protein-to-disease"


def test_cypher_sample_typed_paths_validates_arguments(graph_db):
    with pytest.raises(ValueError, match="Unsupported Cypher query"):
        graph_db.query('CALL pg.sample_typed_paths(["drug-1"], []) RETURN path')
    with pytest.raises(ValueError, match="seed IDs"):
        graph_db.query('CALL pg.sample_typed_paths("drug-1", []) YIELD path RETURN path')
    with pytest.raises(ValueError, match="pattern"):
        graph_db.query('CALL pg.sample_typed_paths(["drug-1"], {}) YIELD path RETURN path')
    with pytest.raises(ValueError, match="expects seed IDs"):
        graph_db.query('CALL pg.sample_typed_paths(["drug-1"]) YIELD path RETURN path')


def test_cypher_parser_rejects_malformed_match_queries(graph_db):
    with pytest.raises(ValueError, match="Unsupported Cypher query"):
        graph_db.query("RETURN n")
    with pytest.raises(ValueError, match="Unsupported Cypher query"):
        graph_db.query('MATCH (d {id: "drug-1"}) RETURN d')
    with pytest.raises(ValueError, match="Unsupported Cypher query"):
        graph_db.query('MATCH (d {id: "drug-1"})-[:rel]->(p)')
    with pytest.raises(ValueError, match="unbound variable"):
        graph_db.query('MATCH (d {id: "drug-1"})-[:rel]->(p) RETURN missing')


def test_split_top_level_args_handles_nested_strings_and_escapes():
    args = _split_top_level_args('["drug,1"], [{"edge_type": "a,\\\"b", "sample_size": 1}]')

    assert args == ['["drug,1"]', '[{"edge_type": "a,\\\"b", "sample_size": 1}]']


def test_query_result_iterates_records():
    result = QueryResult(columns=("n",), records=[{"n": "node"}])

    assert len(result) == 1
    assert list(result) == [{"n": "node"}]


def test_parse_node_scan_returns_none_for_non_node_scan():
    parsed = parse('MATCH (d {id: "drug-1"})-[:rel]->(p) RETURN d, p')

    assert parsed.source_var == "d"


def test_cypher_unsupported_query_raises_clear_error(graph_db):
    with pytest.raises(ValueError, match="Unsupported Cypher query"):
        graph_db.query("MATCH (n) RETURN n")
