import uuid
from typing import List

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class AwerbuchNode(AbstractNode):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors
        self.data = None

        self.parent = None
        self.visited = 0          
        self.was_visited = False  
        self.children_to_ack = set()
        self.sent_to = set()

    def make_action(self, receiver, data):
        return Action(data, receiver, str(uuid.uuid4()))

    def next_unvisited_neighbor(self):
        for n in self.neighbors:
            if n != self.parent and n not in self.sent_to:
                return n
        return None

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data
        msg_type = data.get("message_type")
        sender = data.get("sender_id")

        self.visited += 1
        self.data = msg_type

        outbox = []

        if msg_type == "START" or msg_type == "New":
            if not self.was_visited:
                self.was_visited = True
                self.parent = self.node_id
                self.children_to_ack = set(self.neighbors)

                print(f"[{str(self.node_id)[:6]}] START")

                for neighbor in self.neighbors:
                    outbox.append(self.make_action(neighbor, {
                        "message_type": "VIS",
                        "sender_id": self.node_id
                    }))


        elif msg_type == "VIS":
            print(f"[{str(self.node_id)[:6]}] VIS from {str(sender)[:6]}")
            outbox.append(self.make_action(sender, {
                "message_type": "ACK",
                "sender_id": self.node_id
            }))

        elif msg_type == "ACK":
            print(f"[{str(self.node_id)[:6]}] ACK from {str(sender)[:6]}")

            if sender in self.children_to_ack:
                self.children_to_ack.remove(sender)

            if len(self.children_to_ack) == 0:
                next_node = self.next_unvisited_neighbor()

                if next_node is not None:
                    self.sent_to.add(next_node)
                    print(f"[{str(self.node_id)[:6]}] send TOKEN to {str(next_node)[:6]}")
                    outbox.append(self.make_action(next_node, {
                        "message_type": "TOKEN",
                        "sender_id": self.node_id
                    }))
                elif self.parent != self.node_id:
                    print(f"[{str(self.node_id)[:6]}] return TOKEN to parent {str(self.parent)[:6]}")
                    outbox.append(self.make_action(self.parent, {
                        "message_type": "TOKEN",
                        "sender_id": self.node_id
                    }))

        elif msg_type == "TOKEN":
            print(f"[{str(self.node_id)[:6]}] TOKEN from {str(sender)[:6]}")

            if not self.was_visited:
                self.was_visited = True
                self.parent = sender
                self.children_to_ack = set(n for n in self.neighbors if n != self.parent)

                for neighbor in self.neighbors:
                    if neighbor != self.parent:
                        outbox.append(self.make_action(neighbor, {
                            "message_type": "VIS",
                            "sender_id": self.node_id
                        }))

                if len(self.children_to_ack) == 0:
                    next_node = self.next_unvisited_neighbor()

                    if next_node is not None:
                        self.sent_to.add(next_node)
                        print(f"[{str(self.node_id)[:6]}] send TOKEN to {str(next_node)[:6]}")
                        outbox.append(self.make_action(next_node, {
                            "message_type": "TOKEN",
                            "sender_id": self.node_id
                        }))
                    elif self.parent != self.node_id:
                        print(f"[{str(self.node_id)[:6]}] return TOKEN to parent {str(self.parent)[:6]}")
                        outbox.append(self.make_action(self.parent, {
                            "message_type": "TOKEN",
                            "sender_id": self.node_id
                        }))
            else:
                next_node = self.next_unvisited_neighbor()

                if next_node is not None:
                    self.sent_to.add(next_node)
                    print(f"[{str(self.node_id)[:6]}] send TOKEN to {str(next_node)[:6]}")
                    outbox.append(self.make_action(next_node, {
                        "message_type": "TOKEN",
                        "sender_id": self.node_id
                    }))
                elif self.parent != self.node_id:
                    print(f"[{str(self.node_id)[:6]}] return TOKEN to parent {str(self.parent)[:6]}")
                    outbox.append(self.make_action(self.parent, {
                        "message_type": "TOKEN",
                        "sender_id": self.node_id
                    }))

        return NodeResponse(outbox)