import uuid
from typing import List, Set, Optional

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class RanaNode(AbstractNode):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        self.visited = 0
        self.data = None

        self.clock = 0
        self.active = False
        self.unacked = 0
        self.quiet_since: Optional[int] = None

        self.wave_id = None
        self.wave_time = None
        self.parent = None
        self.waiting: Set[uuid.UUID] = set()

    def short(self, x):
        return str(x)[:6]

    def make_action(self, receiver, data):
        return Action(data, receiver, str(uuid.uuid4()))

    def become_quiet(self):
        if not self.active and self.unacked == 0 and self.quiet_since is None:
            self.clock += 1
            self.quiet_since = self.clock
            print(f"[{self.short(self.node_id)}] QUIET at time={self.quiet_since}")
            return self.start_wave()
        return []

    def start_wave(self):
        self.wave_id = str(uuid.uuid4())
        self.wave_time = self.quiet_since
        self.parent = None
        self.waiting = set(self.neighbors)

        print(f"[{self.short(self.node_id)}] START RANA WAVE time={self.wave_time}")

        if not self.waiting:
            print(f"[{self.short(self.node_id)}] TERMINATION DETECTED")
            return []

        outbox = []
        for n in self.neighbors:
            outbox.append(self.make_action(n, {
                "message_type": "RANA_WAVE",
                "sender_id": self.node_id,
                "wave_id": self.wave_id,
                "wave_time": self.wave_time
            }))
        return outbox

    def process_action(self, message: Action) -> NodeResponse:
        msg = message.data
        msg_type = msg.get("message_type")
        sender = msg.get("sender_id")

        self.visited += 1
        self.data = msg_type
        self.clock += 1

        outbox = []

        if msg_type == "New":
            print(f"[{self.short(self.node_id)}] BASIC COMPUTATION START")
            self.active = True
            self.quiet_since = None

            for n in self.neighbors[:2]:
                self.unacked += 1
                outbox.append(self.make_action(n, {
                    "message_type": "BASIC",
                    "sender_id": self.node_id,
                    "timestamp": self.clock
                }))
                print(f"[{self.short(self.node_id)}] BASIC -> {self.short(n)}")

            self.active = False
            outbox.extend(self.become_quiet())

        elif msg_type == "BASIC":
            incoming_time = msg.get("timestamp", 0)
            self.clock = max(self.clock, incoming_time) + 1

            print(f"[{self.short(self.node_id)}] BASIC from {self.short(sender)}")

            self.active = True
            self.quiet_since = None

            outbox.append(self.make_action(sender, {
                "message_type": "ACK",
                "sender_id": self.node_id,
                "timestamp": self.clock
            }))

            self.active = False
            outbox.extend(self.become_quiet())

        elif msg_type == "ACK":
            incoming_time = msg.get("timestamp", 0)
            self.clock = max(self.clock, incoming_time) + 1

            if self.unacked > 0:
                self.unacked -= 1

            print(f"[{self.short(self.node_id)}] ACK from {self.short(sender)} | unacked={self.unacked}")

            outbox.extend(self.become_quiet())

        elif msg_type == "RANA_WAVE":
            wave_id = msg.get("wave_id")
            wave_time = msg.get("wave_time")

            print(f"[{self.short(self.node_id)}] RANA_WAVE from {self.short(sender)} time={wave_time}")

            can_participate = (
                self.quiet_since is not None
                and self.quiet_since <= wave_time
                and self.unacked == 0
                and not self.active
            )

            if not can_participate:
                outbox.append(self.make_action(sender, {
                    "message_type": "RANA_REJECT",
                    "sender_id": self.node_id,
                    "wave_id": wave_id
                }))
                print(f"[{self.short(self.node_id)}] REJECT wave")
            else:
                self.wave_id = wave_id
                self.wave_time = wave_time
                self.parent = sender
                self.waiting = set(n for n in self.neighbors if n != sender)

                if not self.waiting:
                    outbox.append(self.make_action(self.parent, {
                        "message_type": "RANA_ECHO",
                        "sender_id": self.node_id,
                        "wave_id": self.wave_id
                    }))
                    print(f"[{self.short(self.node_id)}] ECHO -> {self.short(self.parent)}")
                else:
                    for n in self.waiting:
                        outbox.append(self.make_action(n, {
                            "message_type": "RANA_WAVE",
                            "sender_id": self.node_id,
                            "wave_id": self.wave_id,
                            "wave_time": self.wave_time
                        }))

        elif msg_type == "RANA_ECHO":
            wave_id = msg.get("wave_id")

            if wave_id == self.wave_id and sender in self.waiting:
                self.waiting.remove(sender)

            print(f"[{self.short(self.node_id)}] RANA_ECHO from {self.short(sender)}")

            if not self.waiting:
                if self.parent is None:
                    print(f"[{self.short(self.node_id)}] TERMINATION DETECTED")
                else:
                    outbox.append(self.make_action(self.parent, {
                        "message_type": "RANA_ECHO",
                        "sender_id": self.node_id,
                        "wave_id": self.wave_id
                    }))

        elif msg_type == "RANA_REJECT":
            print(f"[{self.short(self.node_id)}] RANA_REJECT from {self.short(sender)}")

            if sender in self.waiting:
                self.waiting.remove(sender)

            if self.parent is None:
                print(f"[{self.short(self.node_id)}] WAVE FAILED, termination not detected")

        return NodeResponse(outbox)