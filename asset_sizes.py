from datetime import timedelta

import obstore
from obstore.auth.planetary_computer import PlanetaryComputerCredentialProvider
from obstore.store import AzureStore, ObjectStore
from pystac import Item
from pystac_client import Client

NUM_SAMPLES = 10
STORAGE_ACCOUNT = "sentinel2l2a01"
CONTAINER_NAME = "sentinel2-l2"
CONTAINER_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}"


def create_azure_store() -> AzureStore:
    """Create a single AzureStore for the sentinel-2-l2a container."""
    return AzureStore(
        account_name=STORAGE_ACCOUNT,
        container_name=CONTAINER_NAME,
        credential_provider=PlanetaryComputerCredentialProvider(CONTAINER_URL),
    )


def get_sample_items(client: Client, num_samples: int) -> list[Item]:
    """Get evenly spaced sample items across the full datetime range."""
    earliest = next(
        client.search(
            collections=["sentinel-2-l2a"], max_items=1, sortby="+datetime"
        ).items()
    )
    latest = next(
        client.search(
            collections=["sentinel-2-l2a"], max_items=1, sortby="-datetime"
        ).items()
    )

    assert earliest.datetime
    assert latest.datetime
    total_seconds = (latest.datetime - earliest.datetime).total_seconds()
    step = total_seconds / (num_samples - 1)

    items = []
    for i in range(num_samples):
        target_dt = earliest.datetime + timedelta(seconds=step * i)
        dt_str = target_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        item = next(
            client.search(
                collections=["sentinel-2-l2a"],
                max_items=1,
                sortby="+datetime",
                datetime=f"{dt_str}/..",
            ).items()
        )
        items.append(item)

    return items


def get_asset_total_size(store: ObjectStore, item: Item) -> tuple[dict[str, int], int]:
    """Get the size of each Azure-hosted asset in an item via HEAD requests."""
    sizes = {}
    prefix = f"{CONTAINER_URL}/"
    for key, asset in item.assets.items():
        href = asset.href
        if href.startswith(prefix):
            path = href[len(prefix) :]
            meta = obstore.head(store, path)
        else:
            continue

        sizes[key] = meta["size"]

    return sizes, sum(sizes.values())


def format_bytes(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def main():
    client = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    store = create_azure_store()

    print(f"Fetching {NUM_SAMPLES} evenly spaced sample items...")
    items = get_sample_items(client, NUM_SAMPLES)

    total_sizes = []
    for i, item in enumerate(items):
        print(f"\n[{i + 1}/{NUM_SAMPLES}] {item.id} ({item.datetime})")
        sizes, item_total = get_asset_total_size(store, item)
        total_sizes.append(item_total)
        for key, size in sorted(sizes.items()):
            print(f"  {key}: {format_bytes(size)}")
        print(f"  TOTAL: {format_bytes(item_total)}")

    avg_size = sum(total_sizes) / len(total_sizes)
    print(f"\nAverage item size: {format_bytes(avg_size)}")


if __name__ == "__main__":
    main()
