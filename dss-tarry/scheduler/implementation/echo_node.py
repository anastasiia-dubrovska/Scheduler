import uuid
from typing import List

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class EchoNode(AbstractNode):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        self.visited = 0
        self.data = None

        self.initiator = False
        self.awaken = False
        self.parent = None
        self.expected_echo = set()
        self.decided = False

    def make_action(self, receiver, data):
        return Action(data, receiver, str(uuid.uuid4()))

    def process_action(self, message: Action) -> NodeResponse:
        msg = message.data
        msg_type = msg.get("message_type")
        sender = msg.get("sender_id")

        self.visited += 1
        self.data = msg_type

        outbox = []

        
        if msg_type == "START" or msg_type == "New":
            if not self.awaken:
                self.awaken = True
                self.initiator = True
                self.parent = None
                self.expected_echo = set(self.neighbors)

                print(f"[{str(self.node_id)[:6]}] START ECHO")

                if len(self.neighbors) == 0:
                    self.decided = True
                    print(f"[{str(self.node_id)[:6]}] DECIDE")
                else:
                    for neighbor in self.neighbors:
                        outbox.append(self.make_action(neighbor, {
                            "message_type": "WAVE",
                            "sender_id": self.node_id
                        }))
                        print(f"[{str(self.node_id)[:6]}] send WAVE to {str(neighbor)[:6]}")

        elif msg_type == "WAVE":
            print(f"[{str(self.node_id)[:6]}] WAVE from {str(sender)[:6]}")

            if not self.awaken:
                self.awaken = True
                self.parent = sender
                self.expected_echo = set(n for n in self.neighbors if n != self.parent)

                if len(self.expected_echo) == 0:
                    outbox.append(self.make_action(self.parent, {
                        "message_type": "ECHO",
                        "sender_id": self.node_id
                    }))
                    print(f"[{str(self.node_id)[:6]}] send ECHO to {str(self.parent)[:6]}")
                else:
                    for neighbor in self.neighbors:
                        if neighbor != self.parent:
                            outbox.append(self.make_action(neighbor, {
                                "message_type": "WAVE",
                                "sender_id": self.node_id
                            }))
                            print(f"[{str(self.node_id)[:6]}] send WAVE to {str(neighbor)[:6]}")
            else:
               
                outbox.append(self.make_action(sender, {
                    "message_type": "ECHO",
                    "sender_id": self.node_id
                }))
                print(f"[{str(self.node_id)[:6]}] duplicate WAVE -> ECHO to {str(sender)[:6]}")

        elif msg_type == "ECHO":
            print(f"[{str(self.node_id)[:6]}] ECHO from {str(sender)[:6]}")
            if sender in self.expected_echo:
                self.expected_echo.remove(sender)

            if len(self.expected_echo) == 0 and not self.decided:
                if self.initiator:
                    self.decided = True
                    print(f"[{str(self.node_id)[:6]}] DECIDE")
                elif self.parent is not None:
                    self.decided = True
                    outbox.append(self.make_action(self.parent, {
                        "message_type": "ECHO",
                        "sender_id": self.node_id
                    }))
                    print(f"[{str(self.node_id)[:6]}] return ECHO to {str(self.parent)[:6]}")

        return NodeResponse(outbox)