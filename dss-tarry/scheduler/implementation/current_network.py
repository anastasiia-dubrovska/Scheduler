import uuid
from pprint import pprint
from typing import List, Dict

from scheduler.abstract.abstract_network import AbstractNetwork
from scheduler.implementation.merlin_segall_node import MerlinSegallNode


class CurrentNetwork(AbstractNetwork):
    NUMBER_OF_NODES = 6

    def __init__(self) -> None:
        self.nodes = []
        ids = [uuid.uuid4() for _ in range(self.NUMBER_OF_NODES)]

        self.destination_id = ids[0]

        self.__get_edges(ids)
        self.__get_weights()

        for node_id in ids:
            self.nodes.append(
                MerlinSegallNode(
                    node_id=node_id,
                    neighbors=self.edges[node_id],
                    weights=self.weights[node_id],
                    destination_id=self.destination_id,
                    total_nodes=self.NUMBER_OF_NODES
                )
            )

        print(f"\nSTART NODE / DESTINATION: {str(self.destination_id)[:6]}\n")

        super().__init__(self.nodes)

    def __get_edges(self, ids: List[uuid.UUID]) -> Dict[uuid.UUID, List[uuid.UUID]]:
        self.edges = {
            ids[0]: [ids[1], ids[2]],
            ids[1]: [ids[0], ids[3], ids[4]],
            ids[2]: [ids[0], ids[5]],
            ids[3]: [ids[1]],
            ids[4]: [ids[1]],
            ids[5]: [ids[2]],
        }

        print("\n===== GRAPH =====")
        pprint(self.edges)
        print("=================\n")

        return self.edges

    def __get_weights(self):
        self.weights = {}

        for node, neighbors in self.edges.items():
            self.weights[node] = {}

            for neighbor in neighbors:
                self.weights[node][neighbor] = 1

        print("===== WEIGHTS =====")
        for node, items in self.weights.items():
            print(str(node)[:6], "->", {str(k)[:6]: v for k, v in items.items()})
        print("===================\n")