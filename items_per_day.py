from datetime import timedelta

from pystac_client import Client

NUM_SAMPLES = 10


def main():
    client = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

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
    step = total_seconds / (NUM_SAMPLES - 1)

    counts = []
    for i in range(NUM_SAMPLES):
        target_dt = earliest.datetime + timedelta(seconds=step * i)
        day_start = target_dt.strftime("%Y-%m-%dT00:00:00Z")
        day_end = target_dt.strftime("%Y-%m-%dT23:59:59Z")

        search = client.search(
            collections=["sentinel-2-l2a"],
            datetime=f"{day_start}/{day_end}",
        )
        count = sum(1 for _ in search.items())
        date_str = target_dt.strftime("%Y-%m-%d")
        print(f"  {date_str}: {count:,} items")
        counts.append(count)

    avg = sum(counts) / len(counts)
    print(f"\nAverage items per day: {avg:,.0f}")


if __name__ == "__main__":
    main()
