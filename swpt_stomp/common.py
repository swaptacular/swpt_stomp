from dataclasses import dataclass
from typing import Callable, List, Any, Optional, TypeVar
import asyncio

_T = TypeVar("_T")
Callback = Callable[[], Any]


@dataclass
class Message:
    id: str
    content_type: str
    body: bytearray


class WatermarkQueue(asyncio.Queue[_T]):
    """A queue that can signal when the given watermarks are reached."""

    low_watermark: int
    high_watermark: int

    def __init__(
            self,
            high_watermark: int,
            low_watermark: Optional[int] = None,
    ):
        if low_watermark is None:
            low_watermark = high_watermark // 4

        if not 0 <= low_watermark <= high_watermark:
            raise ValueError

        super().__init__()
        self.__paused = False
        self.__lw_callbacks: List[Callback] = []
        self.__hw_callbacks: List[Callback] = []
        self.low_watermark: int = low_watermark
        self.high_watermark: int = high_watermark

    def put_nowait(self, item) -> None:
        super().put_nowait(item)
        if not self.__paused and self.qsize() >= self.high_watermark:
            self.__paused = True
            for cb in self.__hw_callbacks:
                cb()

    def task_done(self) -> None:
        super().task_done()
        if self.__paused and self.qsize() <= self.low_watermark:
            self.__paused = False
            for cb in self.__lw_callbacks:
                cb()

    def add_high_watermark_callback(self, cb: Callback) -> None:
        """Add a high watermark callback.

        The callback may be called immediately.
        """
        self.__hw_callbacks.append(cb)
        if self.__paused:
            cb()

    def add_low_watermark_callback(self, cb: Callback) -> None:
        """Add a low watermark callback.

        The callback may be called immediately.
        """
        self.__lw_callbacks.append(cb)
        if not self.__paused:
            cb()

    def remove_high_watermark_callback(self, cb: Callback) -> None:
        """Remove a high watermark callback.

        Raises `ValueError` if the callback has not been registered.
        """
        self.__hw_callbacks.remove(cb)

    def remove_low_watermark_callback(self, cb: Callback) -> None:
        """Remove a low watermark callback.

        Raises `ValueError` if the callback has not been registered.
        """
        self.__lw_callbacks.remove(cb)
