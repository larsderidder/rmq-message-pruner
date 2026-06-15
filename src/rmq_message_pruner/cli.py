import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pika


@dataclass(frozen=True)
class FilterConfig:
    """Configuration for filtering and optionally republishing queue messages."""

    host: str
    port: int
    vhost: str
    user: str
    password: str
    queue: str
    match_terms: List[str]
    match_mode: str
    ignore_case: bool
    republish: bool
    workers: int
    batch_size: int
    max_messages: Optional[int]


def _normalize_text(text: str, ignore_case: bool) -> str:
    """Normalize text for matching based on case sensitivity."""
    return text.lower() if ignore_case else text


def should_drop(text: str, terms: Iterable[str], mode: str, ignore_case: bool) -> bool:
    """Return True when the payload matches the drop rules."""
    if not terms:
        return False
    candidate = _normalize_text(text, ignore_case)
    normalized = [_normalize_text(term, ignore_case) for term in terms]
    if mode == "all":
        return all(term in candidate for term in normalized)
    return any(term in candidate for term in normalized)


def build_params(config: FilterConfig) -> "pika.ConnectionParameters":
    """Create RabbitMQ connection parameters for the filter run."""
    import pika

    credentials = pika.PlainCredentials(config.user, config.password)
    return pika.ConnectionParameters(
        host=config.host,
        port=config.port,
        virtual_host=config.vhost,
        credentials=credentials,
        client_properties={"connection_name": "rmq-message-pruner"},
    )


def process_messages(config: FilterConfig) -> None:
    """Consume messages, filter them, and optionally republish non-matches."""
    import pika

    connection = pika.BlockingConnection(build_params(config))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=max(1, config.batch_size))

    ack_tags: List[int] = []
    processed = 0

    try:
        while True:
            if config.max_messages is not None and processed >= config.max_messages:
                break

            method_frame, properties, body = channel.basic_get(config.queue)
            if not method_frame:
                break

            payload = body.decode("utf-8", errors="replace")
            drop = should_drop(
                payload, config.match_terms, config.match_mode, config.ignore_case
            )

            if not drop and config.republish:
                channel.basic_publish(
                    exchange="",
                    routing_key=config.queue,
                    body=body,
                    properties=properties,
                )

            ack_tags.append(method_frame.delivery_tag)
            processed += 1

            if len(ack_tags) >= config.batch_size:
                channel.basic_ack(delivery_tag=ack_tags[-1], multiple=True)
                ack_tags.clear()

        if ack_tags:
            channel.basic_ack(delivery_tag=ack_tags[-1], multiple=True)
    finally:
        connection.close()


def parse_args(argv: Optional[List[str]] = None) -> FilterConfig:
    """Parse CLI arguments into a FilterConfig."""
    parser = argparse.ArgumentParser(
        description="Filter RabbitMQ queue messages by content"
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5672)
    parser.add_argument("--vhost", default="/")
    parser.add_argument("--user", default="guest")
    parser.add_argument("--password", default="guest")
    parser.add_argument("--queue", required=True)
    parser.add_argument("--match", action="append", default=[])
    parser.add_argument("--match-mode", choices=["any", "all"], default="any")
    parser.add_argument("--ignore-case", action="store_true")
    parser.add_argument("--republish", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-messages", type=int)

    args = parser.parse_args(argv)
    if args.republish and args.workers != 1:
        parser.error("--republish cannot be combined with --workers greater than 1")
    return FilterConfig(
        host=args.host,
        port=args.port,
        vhost=args.vhost,
        user=args.user,
        password=args.password,
        queue=args.queue,
        match_terms=args.match,
        match_mode=args.match_mode,
        ignore_case=args.ignore_case,
        republish=args.republish,
        workers=max(1, args.workers),
        batch_size=max(1, args.batch_size),
        max_messages=args.max_messages,
    )


def main(argv: Optional[List[str]] = None) -> None:
    """CLI entry point."""
    config = parse_args(argv)
    if config.workers == 1:
        process_messages(config)
        return

    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        futures = [
            executor.submit(process_messages, config) for _ in range(config.workers)
        ]
        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
