EDGES = [("A", "B"), ("A", "C"), ("B", "D"), ("C", "D"), ("D", "E")]


def test_build_graph(graphs):
    g = graphs.build_graph(EDGES)
    assert sorted(g["D"]) == ["B", "C", "E"]
    dg = graphs.build_graph([(1, 2)], directed=True)
    assert dg == {1: [2], 2: []}


def test_bfs_and_dfs_order(graphs):
    g = graphs.build_graph(EDGES)
    assert graphs.bfs_order(g, "A") == ["A", "B", "C", "D", "E"]
    assert graphs.dfs_order(g, "A") == ["A", "B", "D", "C", "E"]


def test_dfs_handles_deep_graph(graphs):
    # 5000-deep chain would blow Python's default recursion limit if DFS were recursive.
    g = {i: [i + 1] for i in range(5000)}
    g[5000] = []
    assert len(graphs.dfs_order(g, 0)) == 5001


def test_shortest_path(graphs):
    g = graphs.build_graph(EDGES)
    assert graphs.shortest_path(g, "A", "E") in (["A", "B", "D", "E"], ["A", "C", "D", "E"])
    assert graphs.shortest_path(g, "A", "Z") is None
    assert graphs.shortest_path(g, "A", "A") == ["A"]


def test_num_islands(graphs):
    grid = [
        list("11000"),
        list("11000"),
        list("00100"),
        list("00011"),
    ]
    assert graphs.num_islands(grid) == 3
    assert graphs.num_islands([]) == 0


def test_has_cycle_directed(graphs):
    assert graphs.has_cycle_directed({1: [2], 2: [3], 3: [1]})
    assert not graphs.has_cycle_directed({1: [2, 3], 2: [3], 3: []})
    assert graphs.has_cycle_directed({1: [1]})


def test_topological_sort(graphs):
    # bronze -> silver -> gold style dependency DAG
    dag = {
        "raw_orders": ["stg_orders"],
        "raw_customers": ["dim_customer"],
        "stg_orders": ["fact_sales"],
        "dim_customer": ["fact_sales"],
        "fact_sales": ["agg_daily_sales"],
        "agg_daily_sales": [],
    }
    order = graphs.topological_sort(dag)
    pos = {n: i for i, n in enumerate(order)}
    for u, vs in dag.items():
        for v in vs:
            assert pos[u] < pos[v]
    assert graphs.topological_sort({"a": ["b"], "b": ["a"]}) is None


def test_dijkstra(graphs):
    w = {"A": [("B", 4), ("C", 1)], "C": [("B", 2), ("D", 5)], "B": [("D", 1)], "D": []}
    assert graphs.dijkstra(w, "A") == {"A": 0, "B": 3, "C": 1, "D": 4}


def test_union_find_and_components(graphs):
    assert graphs.count_components(5, [(0, 1), (1, 2), (3, 4)]) == 2
    assert graphs.count_components(3, []) == 3
    uf = graphs.UnionFind()
    uf.union("email:a@x.com", "phone:555")
    uf.union("phone:555", "crm:42")
    assert uf.find("email:a@x.com") == uf.find("crm:42")
