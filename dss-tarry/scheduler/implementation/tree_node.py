import uuid
from typing import List

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class TreeNode(AbstractNode):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        self.visited = 0
        self.data = None

        self.started = False
        self.sent = False
        self.received_from = set()
        self.parent = None
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
            if not self.started:
                self.started = True

               
                if len(self.neighbors) == 1:
                    self.parent = self.neighbors[0]
                    self.sent = True
                    outbox.append(self.make_action(self.parent, {
                        "message_type": "TOKEN",
                        "sender_id": self.node_id
                    }))
                    print(f"[{str(self.node_id)[:6]}] leaf -> TOKEN to {str(self.parent)[:6]}")

        elif msg_type == "TOKEN":
            print(f"[{str(self.node_id)[:6]}] TOKEN from {str(sender)[:6]}")
            self.received_from.add(sender)

            
            if not self.sent:
                remaining = [n for n in self.neighbors if n not in self.received_from]

                if len(remaining) == 1:
                    self.parent = remaining[0]
                    self.sent = True
                    outbox.append(self.make_action(self.parent, {
                        "message_type": "TOKEN",
                        "sender_id": self.node_id
                    }))
                    print(f"[{str(self.node_id)[:6]}] forward TOKEN to {str(self.parent)[:6]}")
                elif len(remaining) == 0 and not self.decided:
                    self.decided = True
                    print(f"[{str(self.node_id)[:6]}] DECIDE")

        return NodeResponse(outbox)