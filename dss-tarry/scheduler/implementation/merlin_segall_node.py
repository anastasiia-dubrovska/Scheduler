import uuid
from typing import List, Dict, Optional

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class MerlinSegallNode(AbstractNode):
    def __init__(
        self,
        node_id: uuid.UUID,
        neighbors: List[uuid.UUID],
        weights: Dict[uuid.UUID, int],
        destination_id: uuid.UUID,
        total_nodes: int
    ):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors
        self.weights = weights
        self.destination_id = destination_id
        self.total_nodes = total_nodes

        self.visited = 0
        self.data = None

        self.dist = 0 if self.node_id == self.destination_id else float("inf")
        self.parent: Optional[uuid.UUID] = None

        self.received_from = set()
        self.finished = False

    def short(self, x):
        if x is None:
            return "None"
        return str(x)[:6]

    def make_action(self, receiver, data):
        return Action(data, receiver, str(uuid.uuid4()))

    def start(self):
        outbox = []

        if self.node_id == self.destination_id:
            print(f"[{self.short(self.node_id)}] DESTINATION START dist=0")

            for n in self.neighbors:
                outbox.append(self.make_action(n, {
                    "message_type": "DIST",
                    "sender_id": self.node_id,
                    "distance": 0
                }))
                print(f"[{self.short(self.node_id)}] send DIST=0 to {self.short(n)}")

        return outbox
    def process_action(self, message: Action) -> NodeResponse:
        msg = message.data
        msg_type = msg.get("message_type")
        sender = msg.get("sender_id")

        self.visited += 1
        self.data = msg_type

        outbox = []

        if msg_type == "New":
            if self.node_id == self.destination_id:
                outbox.extend(self.start())
            else:
                outbox.append(self.make_action(self.destination_id, {
                    "message_type": "START",
                    "sender_id": self.node_id
                }))
                print(
                    f"[{self.short(self.node_id)}] got New, redirect START to "
                    f"{self.short(self.destination_id)}"
                )
            return NodeResponse(outbox)

        if msg_type == "START":
            outbox.extend(self.start())
            return NodeResponse(outbox)

        if msg_type == "DIST":
            incoming_dist = msg.get("distance")

            if sender not in self.weights:
                return NodeResponse([])

            self.received_from.add(sender)
            candidate_dist = incoming_dist + self.weights[sender]

            print(
                f"[{self.short(self.node_id)}] got DIST={incoming_dist} "
                f"from {self.short(sender)} | candidate={candidate_dist}, current={self.dist}"
            )

            if candidate_dist < self.dist:
                self.dist = candidate_dist
                self.parent = sender

                print(
                    f"[{self.short(self.node_id)}] update dist={self.dist}, "
                    f"parent={self.short(self.parent)}"
                )

                for n in self.neighbors:
                    if n != sender:
                        outbox.append(self.make_action(n, {
                            "message_type": "DIST",
                            "sender_id": self.node_id,
                            "distance": self.dist
                        }))
                        print(f"[{self.short(self.node_id)}] send DIST={self.dist} to {self.short(n)}")

            if len(self.received_from) == len(self.neighbors):
                self.finished = True
                print(
                    f"[{self.short(self.node_id)}] FINISHED | "
                    f"dist={self.dist}, parent={self.short(self.parent)}"
                )

        return NodeResponse(outbox)

