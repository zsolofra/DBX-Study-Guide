"""Graph traversal: BFS, DFS, grids, cycle detection, topological sort, Dijkstra, union-find.

Graphs are represented as adjacency lists: Dict[node, List[neighbor]].
"""
import heapq
from collections import defaultdict, deque
from typing import Dict, Hashable, List, Optional, Tuple

Graph = Dict[Hashable, List[Hashable]]


def build_graph(edges: List[Tuple[Hashable, Hashable]], directed: bool = False) -> Graph:
    """Edge list -> adjacency list. O(V + E)."""
    g: Graph = defaultdict(list)
    for u, v in edges:
        g[u].append(v)
        if not directed:
            g[v].append(u)
        else:
            g.setdefault(v, [])
    return dict(g)


def bfs_order(graph: Graph, start: Hashable) -> List[Hashable]:
    """Nodes in BFS order from start (neighbors visited in list order).

    PATTERN: queue + visited set; mark visited when ENQUEUED (not dequeued).
    COMPLEXITY: O(V + E) time, O(V) space.
    """
    visited = {start}
    order = []
    q = deque([start])
    while q:
        node = q.popleft()
        order.append(node)
        for nb in graph.get(node, []):
            if nb not in visited:
                visited.add(nb)
                q.append(nb)
    return order


def dfs_order(graph: Graph, start: Hashable) -> List[Hashable]:
    """Nodes in (preorder) DFS order from start, iterative.

    PATTERN: explicit stack - avoids Python's ~1000 recursion limit on deep graphs.
    Push neighbors reversed so they are visited in list order.
    COMPLEXITY: O(V + E).
    """
    visited = set()
    order = []
    stack = [start]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        order.append(node)
        for nb in reversed(graph.get(node, [])):
            if nb not in visited:
                stack.append(nb)
    return order


def shortest_path(graph: Graph, start: Hashable, goal: Hashable) -> Optional[List[Hashable]]:
    """Fewest-edges path start->goal in an UNWEIGHTED graph, or None.

    PATTERN: BFS + parent pointers; BFS reaches each node first via a shortest path.
    COMPLEXITY: O(V + E).
    """
    parent = {start: None}
    q = deque([start])
    while q:
        node = q.popleft()
        if node == goal:
            path = []
            while node is not None:
                path.append(node)
                node = parent[node]
            return path[::-1]
        for nb in graph.get(node, []):
            if nb not in parent:
                parent[nb] = node
                q.append(nb)
    return None


def num_islands(grid: List[List[str]]) -> int:
    """Count connected groups of '1' (4-directional) in a grid.

    PATTERN: grid as implicit graph; flood-fill with BFS.
    COMPLEXITY: O(rows * cols).
    """
    if not grid:
        return 0
    rows, cols = len(grid), len(grid[0])
    seen = set()
    count = 0
    for r in range(rows):
        for c in range(cols):
            if grid[r][c] == "1" and (r, c) not in seen:
                count += 1
                seen.add((r, c))
                q = deque([(r, c)])
                while q:
                    cr, cc = q.popleft()
                    for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                        nr, nc = cr + dr, cc + dc
                        if 0 <= nr < rows and 0 <= nc < cols and grid[nr][nc] == "1" and (nr, nc) not in seen:
                            seen.add((nr, nc))
                            q.append((nr, nc))
    return count


def has_cycle_directed(graph: Graph) -> bool:
    """True if a directed graph has a cycle.

    PATTERN: DFS three-color (WHITE unvisited, GRAY on stack, BLACK done).
    A GRAY->GRAY edge is a back edge = cycle. Iterative to avoid recursion limits.
    COMPLEXITY: O(V + E).
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in graph}
    for root in graph:
        if color[root] != WHITE:
            continue
        stack = [(root, iter(graph.get(root, [])))]
        color[root] = GRAY
        while stack:
            node, it = stack[-1]
            nxt = next(it, None)
            if nxt is None:
                color[node] = BLACK
                stack.pop()
            elif color.get(nxt, WHITE) == GRAY:
                return True
            elif color.get(nxt, WHITE) == WHITE:
                color[nxt] = GRAY
                stack.append((nxt, iter(graph.get(nxt, []))))
    return False


def topological_sort(graph: Graph) -> Optional[List[Hashable]]:
    """Kahn's algorithm. Returns an ordering or None if there is a cycle.
    Ties broken by sorted node order so the output is deterministic.

    COMPLEXITY: O(V log V + E) with the heap (O(V + E) with a plain queue).
    TALK TRACK: "This is how a Lakeflow/Workflows job or a DLT pipeline decides
    task order: build the dependency DAG, run every node with in-degree 0,
    decrement downstream in-degrees. Leftover nodes means a cycle."
    """
    indeg = {n: 0 for n in graph}
    for u in graph:
        for v in graph[u]:
            indeg[v] = indeg.get(v, 0) + 1
    ready = [n for n, d in indeg.items() if d == 0]
    heapq.heapify(ready)
    order = []
    while ready:
        node = heapq.heappop(ready)
        order.append(node)
        for v in graph.get(node, []):
            indeg[v] -= 1
            if indeg[v] == 0:
                heapq.heappush(ready, v)
    return order if len(order) == len(indeg) else None


def dijkstra(weighted: Dict[Hashable, List[Tuple[Hashable, float]]], start: Hashable) -> Dict[Hashable, float]:
    """Shortest distance from start to every reachable node (non-negative weights).

    PATTERN: min-heap of (dist, node); skip stale heap entries.
    COMPLEXITY: O((V + E) log V).
    """
    dist = {start: 0}
    heap = [(0, start)]
    while heap:
        d, node = heapq.heappop(heap)
        if d > dist.get(node, float("inf")):
            continue
        for nb, w in weighted.get(node, []):
            nd = d + w
            if nd < dist.get(nb, float("inf")):
                dist[nb] = nd
                heapq.heappush(heap, (nd, nb))
    return dist


class UnionFind:
    """Disjoint-set with path compression + union by size: ~O(1) amortized per op.

    TALK TRACK: "Use for connected components / entity resolution - e.g. linking
    customer records that share an email OR phone into one golden customer_id."
    """

    def __init__(self) -> None:
        self.parent: Dict[Hashable, Hashable] = {}
        self.size: Dict[Hashable, int] = {}

    def find(self, x: Hashable) -> Hashable:
        if x not in self.parent:
            self.parent[x] = x
            self.size[x] = 1
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:  # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: Hashable, b: Hashable) -> bool:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]
        return True


def count_components(n: int, edges: List[Tuple[int, int]]) -> int:
    """Number of connected components among nodes 0..n-1."""
    uf = UnionFind()
    for i in range(n):
        uf.find(i)
    merges = sum(1 for a, b in edges if uf.union(a, b))
    return n - merges
