import pytest
from swpt_stomp.common import WatermarkQueue


def test_create_watermark_queue():
    q = WatermarkQueue(4, 2)
    assert q.high_watermark == 4
    assert q.low_watermark == 2

    q = WatermarkQueue(0, 0)
    assert q.high_watermark == 0
    assert q.low_watermark == 0

    q = WatermarkQueue(4)
    assert q.high_watermark == 4
    assert q.low_watermark <= 4

    with pytest.raises(ValueError):
        WatermarkQueue(2, 4)


def test_hit_watermarks():
    lw_cb_called = 0
    hw_cb_called = 0

    def lw_cb():
        nonlocal lw_cb_called
        lw_cb_called += 1

    def hw_cb():
        nonlocal hw_cb_called
        hw_cb_called += 1

    q = WatermarkQueue(4, 2)
    q.add_high_watermark_callback(hw_cb)
    assert hw_cb_called == 0
    assert lw_cb_called == 0

    q.add_low_watermark_callback(lw_cb)
    assert hw_cb_called == 0
    assert lw_cb_called == 1

    q.put_nowait(1)
    q.put_nowait(2)
    q.put_nowait(3)
    assert hw_cb_called == 0
    assert lw_cb_called == 1

    q.put_nowait(4)
    assert hw_cb_called == 1
    assert lw_cb_called == 1

    q.put_nowait(5)
    assert hw_cb_called == 1
    assert lw_cb_called == 1

    q.get_nowait()
    q.get_nowait()
    q.task_done()
    q.task_done()
    assert hw_cb_called == 1
    assert lw_cb_called == 1

    q.get_nowait()
    q.task_done()
    assert hw_cb_called == 1
    assert lw_cb_called == 2

    q.put_nowait(3)
    assert hw_cb_called == 1
    assert lw_cb_called == 2

    q.put_nowait(4)
    assert hw_cb_called == 2
    assert lw_cb_called == 2

    q.add_high_watermark_callback(hw_cb)
    assert hw_cb_called == 3
    assert lw_cb_called == 2


def test_remove_watermark_callbacks():
    cb_called = 0

    def cb():
        nonlocal cb_called
        cb_called += 1

    q = WatermarkQueue(0, 0)
    q.add_high_watermark_callback(cb)
    q.add_low_watermark_callback(cb)
    assert cb_called == 1

    q.remove_high_watermark_callback(cb)
    q.remove_low_watermark_callback(cb)

    q.put_nowait(1)
    q.get_nowait()
    q.task_done()
    assert cb_called == 1
