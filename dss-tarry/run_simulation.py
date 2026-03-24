from scheduler.core.observer import Observer
from scheduler.implementation.current_network import CurrentNetwork
from scheduler.core.action import Action

NETWORK_CLASS = CurrentNetwork

if __name__ == '__main__':
    network = NETWORK_CLASS()

    start_node_obj = network.nodes[0]
    start_node_id = start_node_obj.node_id

    start_node_obj.mailbox.get_actions().append(
        Action(
            {
                "message_type": "START",
                "sender_id": start_node_id
            },
            start_node_id,
            "start-1"
        )
    )

    print(f"\nSTART NODE: {str(start_node_id)[:6]}\n")

    observer = Observer(network)
    observer.run()