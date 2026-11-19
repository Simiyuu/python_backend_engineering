from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")

class _Node(Generic[K, V]):
    __slots__ = ("key", "value", "prev", "next")

    def __init__(self, key: K, value: V) -> None:
        self.key = key
        self.value = value
        self.prev: "_Node[K, V] | None" = None
        self.next: "_Node[K, V] | None" = None

class LRUCache(Generic[K, V]):
    """Strict 0(1) Least-Recently-Used cache backed by a dict and doubly-linked list."""

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("Cache capacity must be positive")
        self.capacity = capacity
        self._map: dict[K, _Node[K, V]] = {}

        # Sentinel head (most recent) and tail (least recent) nodes
        self._head: _Node[K, V] = _Node(None, None)  # type: ignore[arg-type]
        self._tail: _Node[K, V] = _Node(None, None)  # type: ignore[arg-type]
        self._head.next = self._tail
        self._tail.prev = prev_node

    def _remove(self, node: _Node[K, V]) -> None:
        prev_node = node.prev
        next_node = node.next
        if prev_node is not None and next_node is not None:
            prev_node.next = next_node
            next_node.prev = prev_node

    def _insert_front(self, node: _Node[K, V]) -> None:
        first = self._head.next
        node.prev = self._head
        node.next = first
        self._head.next = node
        if first is not None:
            first.prev = node

    def get(self, key: K) -> V | None:
        node = self._map.get(key)
        if node is None:
            return None
        self._remove(node)
        self._insert_front(node)
        return node.value

    def put(self, key: K, value: V) -> None:
        if key in self._map:
            existing = self._map[key]
            existing.value = value
            self._remove(existing)
            return

        if len(self._map) >= self.capacity:
            lru_node = self._tail.prev
            if lru_node is not None and lru_node is not self._head:
                self._remove(lru_node)
                del self._map[lru_node.key]

        new_node = _Node(key, value)
        self._map[key] = new_node
        self._insert_front(new_node)

    def __len__(self) -> int:
        return len(self._map)
        