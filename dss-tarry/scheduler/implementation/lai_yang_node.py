import uuid
import random
from typing import List, Dict, Any

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class LaiYangNode(AbstractNode):
    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        self.visited = 0
        self.data = None

        self.balance = random.randint(1, 9)

        self.color = "white"
        self.snapshot_taken = False
        self.is_initiator = False
        self.snapshot_id = None
        self.parent = None


        self.white_sent: Dict[uuid.UUID, List[str]] = {n: [] for n in neighbors}
        self.white_recv: Dict[uuid.UUID, List[str]] = {n: [] for n in neighbors}

        self.white_sent_count_before_snapshot: Dict[uuid.UUID, int] = {n: 0 for n in neighbors}

        
        self.expected_white_from_sender: Dict[uuid.UUID, int | None] = {n: None for n in neighbors}
        self.closed_incoming: Dict[uuid.UUID, bool] = {n: False for n in neighbors}


        self.local_snapshot: Dict[str, Any] | None = None
        self.channel_state: Dict[str, List[str]] = {}

        self.reports: Dict[str, Dict[str, Any]] = {}

    def make_action(self, receiver, data):
        return Action(data, receiver, str(uuid.uuid4()))

    def _short(self, x):
        return str(x)[:6]

    def _take_snapshot(self, snapshot_id: str, initiator_id: uuid.UUID):
        if self.snapshot_taken:
            return []

        self.snapshot_taken = True
        self.color = "red"
        self.snapshot_id = snapshot_id

        self.local_snapshot = {
            "node": str(self.node_id),
            "balance": self.balance,
            "color": self.color,
        }

        for n in self.neighbors:
            self.white_sent_count_before_snapshot[n] = len(self.white_sent[n])

        print(f"[{self._short(self.node_id)}] TAKE SNAPSHOT | balance={self.balance}")

        outbox = []

        
        for n in self.neighbors:
            outbox.append(self.make_action(n, {
                "message_type": "CONTROL",
                "sender_id": self.node_id,
                "snapshot_id": snapshot_id,
                "initiator_id": initiator_id,
                "white_sent_count": self.white_sent_count_before_snapshot[n]
            }))


        self.parent = initiator_id
        outbox.extend(self._maybe_send_report())

        return outbox

    def _maybe_send_report(self):

        for n in self.neighbors:
            exp = self.expected_white_from_sender[n]
            if exp is None:
                return []
            if len(self.white_recv[n]) < exp:
                return []
            if not self.closed_incoming[n]:
                self.closed_incoming[n] = True
                key = f"{self._short(n)}->{self._short(self.node_id)}"
                self.channel_state[key] = self.white_recv[n][:exp]

        if self.local_snapshot is None:
            return []

        report = {
            "message_type": "SNAPSHOT_REPORT",
            "sender_id": self.node_id,
            "snapshot_id": self.snapshot_id,
            "local_snapshot": self.local_snapshot,
            "channel_state": self.channel_state
        }


        if self.is_initiator:
            self.reports[str(self.node_id)] = {
                "local_snapshot": self.local_snapshot,
                "channel_state": self.channel_state
            }
            print(f"[{self._short(self.node_id)}] LOCAL REPORT READY")
            return []

        if self.parent is not None:
            print(f"[{self._short(self.node_id)}] SEND REPORT -> {self._short(self.parent)}")
            return [self.make_action(self.parent, report)]

        return []

    def _send_basic(self):
        if not self.neighbors:
            return []

        receiver = random.choice(self.neighbors)
        payload_id = str(uuid.uuid4())[:8]

     
        red_flag = self.color == "red"

        if not red_flag:
            self.white_sent[receiver].append(payload_id)

        print(
            f"[{self._short(self.node_id)}] BASIC -> {self._short(receiver)} "
            f"| payload={payload_id} | red={red_flag}"
        )

        return [self.make_action(receiver, {
            "message_type": "BASIC",
            "sender_id": self.node_id,
            "payload_id": payload_id,
            "red": red_flag,
            "snapshot_id": self.snapshot_id
        })]

    def process_action(self, message: Action) -> NodeResponse:
        msg = message.data
        msg_type = msg.get("message_type")
        sender = msg.get("sender_id")

        self.visited += 1
        self.data = msg_type

        outbox = []

        if msg_type == "New" or msg_type == "START_SNAPSHOT":
            if not self.snapshot_taken:
                self.is_initiator = True
                snap_id = str(uuid.uuid4())
                print(f"[{self._short(self.node_id)}] INIT SNAPSHOT {snap_id[:8]}")
                outbox.extend(self._take_snapshot(snap_id, self.node_id))

                for _ in range(min(2, max(1, len(self.neighbors)))):
                    outbox.extend(self._send_basic())

        elif msg_type == "BASIC":
            payload_id = msg.get("payload_id")
            red_flag = msg.get("red", False)
            snap_id = msg.get("snapshot_id")


            if (not self.snapshot_taken) and red_flag:
                initiator_id = sender if msg.get("snapshot_id") is None else msg.get("sender_id")
                outbox.extend(self._take_snapshot(snap_id or str(uuid.uuid4()), initiator_id))

            if not red_flag:
                if sender in self.white_recv:
                    self.white_recv[sender].append(payload_id)

            print(
                f"[{self._short(self.node_id)}] RECV BASIC from {self._short(sender)} "
                f"| payload={payload_id} | red={red_flag}"
            )

            self.balance += 1

            if self.neighbors and random.random() < 0.35:
                outbox.extend(self._send_basic())

            outbox.extend(self._maybe_send_report())

        elif msg_type == "CONTROL":
            snapshot_id = msg.get("snapshot_id")
            initiator_id = msg.get("initiator_id")
            white_count = msg.get("white_sent_count", 0)


            if not self.snapshot_taken:
                outbox.extend(self._take_snapshot(snapshot_id, initiator_id))

            self.expected_white_from_sender[sender] = white_count
            print(
                f"[{self._short(self.node_id)}] CONTROL from {self._short(sender)} "
                f"| white_sent_count={white_count}"
            )

            outbox.extend(self._maybe_send_report())

        elif msg_type == "SNAPSHOT_REPORT":

            local_snapshot = msg.get("local_snapshot")
            channel_state = msg.get("channel_state")

            if self.is_initiator:
                self.reports[str(sender)] = {
                    "local_snapshot": local_snapshot,
                    "channel_state": channel_state
                }
                print(f"[{self._short(self.node_id)}] GOT REPORT from {self._short(sender)}")


                if str(self.node_id) not in self.reports and self.local_snapshot is not None:
                    self.reports[str(self.node_id)] = {
                        "local_snapshot": self.local_snapshot,
                        "channel_state": self.channel_state
                    }


                print(f"[{self._short(self.node_id)}] REPORT COUNT = {len(self.reports)}")
                print(f"[{self._short(self.node_id)}] CURRENT SNAPSHOT = {self.reports}")
            else:

                pass

        return NodeResponse(outbox)