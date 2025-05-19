import logging

logger = logging.getLogger("postgres_to_es")

logger.setLevel(logging.INFO)


log_format = logging.Formatter(
    "[%(asctime)s | %(levelname)s]: %(message)s", datefmt="%m.%d.%Y %H:%M:%S"
)

console_out = logging.StreamHandler()
console_out.setFormatter(log_format)

logger.addHandler(console_out)
