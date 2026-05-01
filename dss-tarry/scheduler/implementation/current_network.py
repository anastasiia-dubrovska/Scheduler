import uuid
from pprint import pprint
from typing import List, Dict

from scheduler.abstract.abstract_network import AbstractNetwork

#from scheduler.implementation.rana_node import RanaNode
from scheduler.implementation.safra_node import SafraNode


class CurrentNetwork(AbstractNetwork):
    NUMBER_OF_NODES = 6

    def __init__(self) -> None:
        self.nodes = []
        ids = [uuid.uuid4() for _ in range(self.NUMBER_OF_NODES)]

        self.__get_edges(ids)

        for i, node_id in enumerate(ids):
            # ===== Rana algorithm =====
            #self.nodes.append(RanaNode(node_id, self.edges[node_id]))

            # ===== Safra algorithm =====
            ring_next = ids[(i + 1) % len(ids)]
            self.nodes.append(SafraNode(node_id, self.edges[node_id], ring_next))

        super().__init__(self.nodes)

    def __get_edges(self, ids: List[uuid.UUID]) -> Dict[uuid.UUID, List[uuid.UUID]]:
        self.edges = {
            ids[0]: [ids[1], ids[2]],
            ids[1]: [ids[0], ids[3], ids[4]],
            ids[2]: [ids[0], ids[5]],
            ids[3]: [ids[1]],
            ids[4]: [ids[1]],
            ids[5]: [ids[2]]
        }

        print("\n===== GRAPH =====")
        pprint(self.edges)
        print("=================\n")

        return self.edges