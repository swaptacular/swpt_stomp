import pytest


def test_calc_bin_routing_key():
    from swpt_stomp.process_messages import _calc_bin_routing_key
    assert _calc_bin_routing_key(123) == \
        '1.1.1.1.1.1.0.0.0.0.0.1.0.0.0.0.0.1.1.0.0.0.1.1'
    assert _calc_bin_routing_key(-123) == \
        '1.1.0.0.0.0.1.1.1.1.1.1.1.1.1.0.1.0.1.0.1.1.1.1'
    assert _calc_bin_routing_key(123, 456) == \
        '0.0.0.0.1.0.0.0.0.1.0.0.0.1.0.0.0.0.1.1.0.1.0.0'

    with pytest.raises(OverflowError):
        _calc_bin_routing_key(99999999999999999999999999999999999)
    with pytest.raises(Exception):
        _calc_bin_routing_key('')
