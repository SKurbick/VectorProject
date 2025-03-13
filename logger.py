import os
import inspect
import datetime
from loguru import logger as loguru_logger
from functools import wraps

from notification import telegram

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logging")
os.makedirs(LOG_DIR, exist_ok=True)


def get_logger():
    """
    Настраивает логгер для модуля, вызвавшего эту функцию, создавая папку с логами.
    """
    frame = inspect.stack()[1]
    module_name = os.path.splitext(os.path.basename(frame.filename))[0]

    module_log_dir = os.path.join(LOG_DIR, module_name)
    os.makedirs(module_log_dir, exist_ok=True)

    log_file = os.path.join(module_log_dir, f"{module_name}.log")
    loguru_logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="10 MB",
        compression="zip",
        level="DEBUG",
        enqueue=True,
    )
    return loguru_logger


def log_job(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        job_name = func.__name__
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
        job_file = __file__

        job_log_dir = os.path.join(LOG_DIR, job_name)
        os.makedirs(job_log_dir, exist_ok=True)
        log_filename = os.path.join(job_log_dir, f"{job_name}_{timestamp}.log")

        filter_func = lambda record: record["extra"].get("job") == job_name

        sink_id = loguru_logger.add(
            log_filename,
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
            level="INFO",
            filter=filter_func
        )

        with loguru_logger.contextualize(job=job_name):
            loguru_logger.info(f"Начало выполнения задачи '{job_name}' в файле {job_file} (время: {timestamp})")
            try:
                result = await func(*args, **kwargs)
                loguru_logger.info(f"Задача '{job_name}' завершена успешно")
                return result
            except Exception as e:
                loguru_logger.error(f"Ошибка в задаче '{job_name}': {e}")
                record = {"time": datetime.datetime.now().strftime("%Y-%m-%d at %H:%M:%S"), "level": "ERROR",
                          "name": func.__module__, "function": func.__name__,
                          "line": inspect.currentframe().f_back.f_lineno, "message": str(e), }
                error_message = (
                    f"VectorProject: <b><u>{job_name.upper()}</u></b> | {record['time']} | {record['level']} | {record['name']}:"
                    f" {record['function']}:{record['line']} - {record['message']}"
                )
                await telegram(error_message)
                raise
            finally:
                loguru_logger.remove(sink_id)

    return wrapper


app_logger = get_logger()
