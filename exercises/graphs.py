# AUTO-GENERATED EXERCISE STUBS - implement each function, then run the tests.
"""Graph traversal: BFS, DFS, grids, cycle detection, topological sort, Dijkstra, union-find.

Graphs are represented as adjacency lists: Dict[node, List[neighbor]].
"""
import heapq
from collections import defaultdict, deque
from typing import Dict, Hashable, List, Optional, Tuple
Graph = Dict[Hashable, List[Hashable]]

def build_graph(edges: List[Tuple[Hashable, Hashable]], directed: bool=False) -> Graph:
    """Edge list -> adjacency list. O(V + E).
    """
    raise NotImplementedError('TODO: implement me')

def bfs_order(graph: Graph, start: Hashable) -> List[Hashable]:
    """Nodes in BFS order from start (neighbors visited in list order).

    COMPLEXITY: O(V + E) time, O(V) space.
    """
    raise NotImplementedError('TODO: implement me')

def dfs_order(graph: Graph, start: Hashable) -> List[Hashable]:
    """Nodes in (preorder) DFS order from start, iterative.

    COMPLEXITY: O(V + E).
    """
    raise NotImplementedError('TODO: implement me')

def shortest_path(graph: Graph, start: Hashable, goal: Hashable) -> Optional[List[Hashable]]:
    """Fewest-edges path start->goal in an UNWEIGHTED graph, or None.

    COMPLEXITY: O(V + E).
    """
    raise NotImplementedError('TODO: implement me')

def num_islands(grid: List[List[str]]) -> int:
    """Count connected groups of '1' (4-directional) in a grid.

    COMPLEXITY: O(rows * cols).
    """
    raise NotImplementedError('TODO: implement me')

def has_cycle_directed(graph: Graph) -> bool:
    """True if a directed graph has a cycle.

    COMPLEXITY: O(V + E).
    """
    raise NotImplementedError('TODO: implement me')

def topological_sort(graph: Graph) -> Optional[List[Hashable]]:
    """Kahn's algorithm. Returns an ordering or None if there is a cycle.
    Ties broken by sorted node order so the output is deterministic.

    COMPLEXITY: O(V log V + E) with the heap (O(V + E) with a plain queue).
    """
    raise NotImplementedError('TODO: implement me')

def dijkstra(weighted: Dict[Hashable, List[Tuple[Hashable, float]]], start: Hashable) -> Dict[Hashable, float]:
    """Shortest distance from start to every reachable node (non-negative weights).

    COMPLEXITY: O((V + E) log V).
    """
    raise NotImplementedError('TODO: implement me')

class UnionFind:
    """Disjoint-set with path compression + union by size: ~O(1) amortized per op.

    TALK TRACK: "Use for connected components / entity resolution - e.g. linking
    customer records that share an email OR phone into one golden customer_id."
    """

    def __init__(self) -> None:
        raise NotImplementedError('TODO: implement me')

    def find(self, x: Hashable) -> Hashable:
        raise NotImplementedError('TODO: implement me')

    def union(self, a: Hashable, b: Hashable) -> bool:
        raise NotImplementedError('TODO: implement me')

def count_components(n: int, edges: List[Tuple[int, int]]) -> int:
    """Number of connected components among nodes 0..n-1.
    """
    raise NotImplementedError('TODO: implement me')
