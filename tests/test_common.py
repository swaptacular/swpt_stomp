import pytest
from swpt_stomp.common import (
    WatermarkQueue,
    get_peer_serial_number,
    terminate_queue,
)
from unittest.mock import NonCallableMock, Mock


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

    with pytest.raises(ValueError):
        q.remove_low_watermark_callback("invalid")

    with pytest.raises(ValueError):
        q.remove_high_watermark_callback("invalid")


def test_watermark_queue_put(loop):
    cb_called = 0

    def cb():
        nonlocal cb_called
        cb_called += 1

    q = WatermarkQueue(0, 0)
    q.add_high_watermark_callback(cb)

    assert cb_called == 0
    loop.run_until_complete(q.put("item"))
    assert cb_called == 1


def test_get_peer_serial_number():
    data = {
        "issuer": (
            (("countryName", "IL"),),
            (("organizationName", "StartCom Ltd."),),
            (
                (
                    "organizationalUnitName",
                    "Secure Digital Certificate Signing",
                ),
            ),
            (
                (
                    "commonName",
                    "StartCom Class 2 Primary Intermediate Server CA",
                ),
            ),
        ),
        "notAfter": "Nov 22 08:15:19 2013 GMT",
        "notBefore": "Nov 21 03:09:52 2011 GMT",
        "serialNumber": "95F0",
        "subject": (
            (("description", "571208-SLe257oHY9fVQ07Z"),),
            (("countryName", "US"),),
            (("stateOrProvinceName", "California"),),
            (("localityName", "San Francisco"),),
            (("organizationName", "Swaptacular Nodes Registry"),),
            (("commonName", "*.eff.org"),),
            (("serialNumber", "1234abcd"),),
            (("emailAddress", "hostmaster@eff.org"),),
        ),
        "subjectAltName": (("DNS", "*.eff.org"), ("DNS", "eff.org")),
        "version": 3,
    }
    transport = NonCallableMock(
        get_extra_info=Mock(return_value=data),
    )
    assert get_peer_serial_number(transport) == "1234abcd"

    data["subject"] = (
        (("description", "571208-SLe257oHY9fVQ07Z"),),
        (("countryName", "US"),),
        (("stateOrProvinceName", "California"),),
        (("localityName", "San Francisco"),),
        (("organizationName", "SOMETHING ELSE"),),
        (("commonName", "*.eff.org"),),
        (("serialNumber", "1234abcd"),),
        (("emailAddress", "hostmaster@eff.org"),),
    )
    transport = NonCallableMock(
        get_extra_info=Mock(return_value=data),
    )
    assert get_peer_serial_number(transport) is None

    data["subject"] = (
        (("description", "571208-SLe257oHY9fVQ07Z"),),
        (("countryName", "US"),),
        (("stateOrProvinceName", "California"),),
        (("localityName", "San Francisco"),),
        (("organizationName", "Swaptacular Nodes Registry"),),
        (("commonName", "*.eff.org"),),
        (("serialNumber", "1234abcd"), ("serialNumber", "00000000")),
        (("emailAddress", "hostmaster@eff.org"),),
    )
    transport = NonCallableMock(
        get_extra_info=Mock(return_value=data),
    )
    assert get_peer_serial_number(transport) is None


@pytest.mark.asyncio
async def test_terminate_queue():
    import asyncio

    loop = asyncio.get_running_loop()
    queue = asyncio.Queue(1)
    terminate_queue(queue, "first")
    assert queue.qsize() == 1
    terminate_queue(queue, "second")
    assert queue.qsize() == 1
    loop.call_soon(queue.get_nowait)
    await asyncio.sleep(0.2)
    assert queue.qsize() == 1
    assert queue.get_nowait() == "second"
