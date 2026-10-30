from n0_kernelbench_oj.adapter import _matches_kernelbench_mean as agrees

def test_legitimate_three_digit_rounding():
    assert agrees(2.334, 2.33)
    assert agrees(8.254975986480712, 8.25)
    assert agrees(7.263808059692383, 7.26)

def test_inconsistent_or_invalid_summary_is_rejected():
    assert not agrees(2.334, 2.35)
    assert not agrees(2.334, 0.0)
    assert not agrees(float('nan'), 2.33)
    assert not agrees(float('inf'), 2.33)
