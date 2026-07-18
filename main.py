"""Application entry point for StreamForge."""


def create_app() -> dict[str, str]:
    """Create the initial application configuration.

    Keeping setup logic in one function makes the project easier to expand
    when producer, consumer, dashboard, and Docker pieces are connected.
    """
    return {
        "name": "StreamForge",
        "status": "ready",
    }


def main() -> None:
    """Run the StreamForge application."""
    app = create_app()
    print(f"{app['name']} is {app['status']}.")


if __name__ == "__main__":
    main()
