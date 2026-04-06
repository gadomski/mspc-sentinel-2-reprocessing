from pystac import Item
from pystac_client import Client as StacClient


class Client:
    def __init__(self) -> None:
        self.client = StacClient.open(
            "https://planetarycomputer.microsoft.com/api/stac/v1"
        )

    def search_one(
        self, sortby: str | None = None, datetime: str | None = None
    ) -> Item:
        return next(
            self.client.search(
                collections=["sentinel-2-l2a"],
                max_items=1,
                sortby=sortby,
                datetime=datetime,
            ).items()
        )


def find_transitions(
    client: Client, left: Item, right: Item, depth: int = 0
) -> list[tuple[Item, Item]]:
    """Find all processing baseline transition points between two items using binary search."""
    left_baseline = left.properties["s2:processing_baseline"]
    right_baseline = right.properties["s2:processing_baseline"]

    indent = "  " * depth
    print(
        f"{indent}Searching between {left.datetime} ({left_baseline}) and {right.datetime} ({right_baseline})"
    )

    if left_baseline == right_baseline:
        return []

    assert left.datetime
    assert right.datetime
    mid_dt = left.datetime + (right.datetime - left.datetime) / 2
    mid_str = mid_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"{indent}  Querying midpoint {mid_str}...")
    mid_item = client.search_one(sortby="+datetime", datetime=f"{mid_str}/..")

    mid_baseline = mid_item.properties["s2:processing_baseline"]
    print(f"{indent}  Found {mid_item.id} with baseline {mid_baseline}")

    if mid_item.id == left.id or mid_item.id == right.id:
        print(f"{indent}  -> Transition found!")
        return [(left, right)]

    transitions = []
    if left_baseline != mid_baseline:
        transitions.extend(find_transitions(client, left, mid_item, depth + 1))
    if mid_baseline != right_baseline:
        transitions.extend(find_transitions(client, mid_item, right, depth + 1))

    return transitions


def main():
    client = Client()
    start_item = client.search_one(sortby="+datetime")
    end_item = client.search_one(sortby="-datetime")

    print(
        f"Start: {start_item.properties['s2:processing_baseline']} ({start_item.datetime})"
    )
    print(
        f"End:   {end_item.properties['s2:processing_baseline']} ({end_item.datetime})"
    )

    transitions = find_transitions(client, start_item, end_item)

    for before, after in transitions:
        print(
            f"\nTransition: {before.properties['s2:processing_baseline']} -> {after.properties['s2:processing_baseline']}"
        )
        print(f"  Before: {before.id} ({before.datetime})")
        print(f"  After:  {after.id} ({after.datetime})")


if __name__ == "__main__":
    main()
