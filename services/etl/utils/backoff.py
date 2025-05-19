import time
import traceback
from functools import wraps
from typing import Any, Callable

from utils.logging_settings import logger


def backoff(  # noqa: CCR001
    start_sleep_time: float = 0.1,
    factor: int = 2,
    border_sleep_time: int = 10,
    exceptions: tuple = (Exception,),
) -> Callable:
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param exceptions: кортеж исключений
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания
    на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func: Callable) -> Callable:  # noqa: CCR001
        attempts = 0
        time_to_wait = start_sleep_time

        @wraps(func)
        def inner(*args: Any, **kwargs: Any) -> Callable:
            nonlocal attempts
            nonlocal time_to_wait

            while True:
                try:
                    logger.info(f"Попытка №{attempts} для {func.__name__}")
                    call = func(*args, **kwargs)
                    attempts = 0
                    return call
                except exceptions:
                    logger.error(traceback.format_exc())  # noqa: T201
                    attempts += 1
                    time.sleep(time_to_wait)
                    if time_to_wait < border_sleep_time:
                        time_to_wait = start_sleep_time * (factor**attempts)
                    else:
                        time_to_wait = 10

        return inner

    return func_wrapper
