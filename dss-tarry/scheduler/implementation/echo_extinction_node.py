import uuid
from typing import List, Dict, Set, Optional

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class EchoExtinctionNode(AbstractNode):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors


        self.visited = 0
        self.data = None

        self.current_wave_id: Optional[str] = None
        self.parent: Optional[uuid.UUID] = None
        self.children_waiting: Set[uuid.UUID] = set()
        self.decided = False
        self.leader_id: Optional[str] = None


        self.seen_from: Dict[str, Set[uuid.UUID]] = {}

    def make_action(self, receiver, data):
        return Action(data, receiver, str(uuid.uuid4()))

    def short(self, value):
        return str(value)[:6]

    def better_wave(self, new_wave_id: str) -> bool:
        if self.current_wave_id is None:
            return True
        return new_wave_id < self.current_wave_id   

    def reset_for_new_wave(self, wave_id: str, parent: Optional[uuid.UUID]):
        self.current_wave_id = wave_id
        self.parent = parent
        self.children_waiting = set()
        if wave_id not in self.seen_from:
            self.seen_from[wave_id] = set()

    def start_wave(self):
        wave_id = str(self.node_id)
        self.reset_for_new_wave(wave_id, None)

        print(f"[{self.short(self.node_id)}] START ELECTION wave={self.short(wave_id)}")

        outbox = []

        if len(self.neighbors) == 0:
            self.decided = True
            self.leader_id = wave_id
            print(f"[{self.short(self.node_id)}] ELECTED leader={self.short(self.leader_id)}")
            return outbox

        self.children_waiting = set(self.neighbors)

        for neighbor in self.neighbors:
            outbox.append(self.make_action(neighbor, {
                "message_type": "WAVE",
                "sender_id": self.node_id,
                "wave_id": wave_id
            }))

        return outbox

    def send_echo_to_parent(self):
        if self.parent is None:
            self.decided = True
            self.leader_id = self.current_wave_id
            print(f"[{self.short(self.node_id)}] ELECTED leader={self.short(self.leader_id)}")
            return []

        print(
            f"[{self.short(self.node_id)}] send ECHO to {self.short(self.parent)} "
            f"for wave={self.short(self.current_wave_id)}"
        )

        return [self.make_action(self.parent, {
            "message_type": "ECHO",
            "sender_id": self.node_id,
            "wave_id": self.current_wave_id
        })]

    def process_action(self, message: Action) -> NodeResponse:
        msg = message.data
        msg_type = msg.get("message_type")
        sender = msg.get("sender_id")
        wave_id = msg.get("wave_id")

        self.visited += 1
        self.data = msg_type

        outbox = []


        if msg_type == "START" or msg_type == "New":
            if self.current_wave_id is None and not self.decided:
                outbox.extend(self.start_wave())
            return NodeResponse(outbox)

        if msg_type == "WAVE":
            print(
                f"[{self.short(self.node_id)}] got WAVE from {self.short(sender)} "
                f"wave={self.short(wave_id)}"
            )

            if self.better_wave(wave_id):
                self.reset_for_new_wave(wave_id, sender)
                self.seen_from[wave_id].add(sender)

                forward_neighbors = [n for n in self.neighbors if n != sender]
                self.children_waiting = set(forward_neighbors)

                if len(forward_neighbors) == 0:
                    outbox.extend(self.send_echo_to_parent())
                else:
                    for neighbor in forward_neighbors:
                        outbox.append(self.make_action(neighbor, {
                            "message_type": "WAVE",
                            "sender_id": self.node_id,
                            "wave_id": wave_id
                        }))

            elif wave_id == self.current_wave_id:

                outbox.append(self.make_action(sender, {
                    "message_type": "ECHO",
                    "sender_id": self.node_id,
                    "wave_id": wave_id
                }))

            else:

                outbox.append(self.make_action(sender, {
                    "message_type": "ECHO",
                    "sender_id": self.node_id,
                    "wave_id": wave_id
                }))

        elif msg_type == "ECHO":
            print(
                f"[{self.short(self.node_id)}] got ECHO from {self.short(sender)} "
                f"wave={self.short(wave_id)}"
            )


            if wave_id != self.current_wave_id:
                return NodeResponse([])

            if sender in self.children_waiting:
                self.children_waiting.remove(sender)

            if len(self.children_waiting) == 0:
                outbox.extend(self.send_echo_to_parent())

        return NodeResponse(outbox)