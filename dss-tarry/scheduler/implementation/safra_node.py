import uuid
from typing import List, Optional

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class SafraNode(AbstractNode):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID], ring_next: Optional[uuid.UUID] = None):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors
        self.ring_next = ring_next

        self.visited = 0
        self.data = None

        self.active = False
        self.color = "white"
        self.counter = 0

        self.is_initiator = False
        self.token_started = False

    def short(self, x):
        return str(x)[:6] if x is not None else "None"

    def make_action(self, receiver, data):
        return Action(data, receiver, str(uuid.uuid4()))

    def send_basic(self, receiver):
        self.counter += 1
        self.color = "black"

        print(f"[{self.short(self.node_id)}] BASIC -> {self.short(receiver)} | counter={self.counter}")

        return self.make_action(receiver, {
            "message_type": "BASIC",
            "sender_id": self.node_id
        })

    def forward_token(self, token_color, token_counter):
        token_counter += self.counter

        if self.color == "black":
            token_color = "black"

        print(
            f"[{self.short(self.node_id)}] TOKEN pass -> {self.short(self.ring_next)} "
            f"| token_color={token_color}, token_counter={token_counter}"
        )

        self.color = "white"
        self.counter = 0

        return self.make_action(self.ring_next, {
            "message_type": "TOKEN",
            "sender_id": self.node_id,
            "token_color": token_color,
            "token_counter": token_counter
        })

    def process_action(self, message: Action) -> NodeResponse:
        msg = message.data
        msg_type = msg.get("message_type")
        sender = msg.get("sender_id")

        self.visited += 1
        self.data = msg_type

        outbox = []

        if msg_type == "New":
            print(f"[{self.short(self.node_id)}] BASIC COMPUTATION START")

            self.active = True

            for n in self.neighbors[:2]:
                outbox.append(self.send_basic(n))

            self.active = False

            if not self.token_started:
                self.is_initiator = True
                self.token_started = True

                print(f"[{self.short(self.node_id)}] START SAFRA TOKEN")

                outbox.append(self.make_action(self.ring_next, {
                    "message_type": "TOKEN",
                    "sender_id": self.node_id,
                    "token_color": "white",
                    "token_counter": 0
                }))

        elif msg_type == "BASIC":
            self.counter -= 1
            self.active = True

            print(f"[{self.short(self.node_id)}] BASIC from {self.short(sender)} | counter={self.counter}")

            self.active = False

        elif msg_type == "TOKEN":
            token_color = msg.get("token_color")
            token_counter = msg.get("token_counter", 0)

            print(
                f"[{self.short(self.node_id)}] TOKEN received "
                f"| token_color={token_color}, token_counter={token_counter}, "
                f"local_color={self.color}, local_counter={self.counter}, active={self.active}"
            )

            if self.is_initiator:
                total = token_counter + self.counter
                final_color = "black" if self.color == "black" or token_color == "black" else "white"

                if not self.active and final_color == "white" and total == 0:
                    print(f"[{self.short(self.node_id)}] TERMINATION DETECTED BY SAFRA")
                    self.color = "white"
                    self.counter = 0
                else:
                    self.color = "white"
                    self.counter = 0
                    outbox.append(self.make_action(self.ring_next, {
                        "message_type": "TOKEN",
                        "sender_id": self.node_id,
                        "token_color": "white",
                        "token_counter": 0
                    }))
                    print(f"[{self.short(self.node_id)}] TOKEN RESTART")
            else:
                if not self.active:
                    outbox.append(self.forward_token(token_color, token_counter))
                else:
                    print(f"[{self.short(self.node_id)}] ACTIVE, token delayed")

        return NodeResponse(outbox)