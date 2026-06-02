def pytest_addoption(parser):
    parser.addoption(
        "--no-cache-dir",
        action="store_true",
        default=False,
        help="Disable pytest cache (alias accepted for parity with pip --no-cache-dir).",
    )


def pytest_configure(config):
    if config.getoption("--no-cache-dir"):
        config.option.cacheprovider = False
