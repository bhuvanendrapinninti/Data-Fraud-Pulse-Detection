def is_high_amount(amount, threshold=50000):
    if amount < 0:
        raise ValueError("amount must be non-negative")
    return amount >= threshold

def test_threshold_boundary():
    assert is_high_amount(50000)
    assert is_high_amount(50000.01)
    assert not is_high_amount(49999.99)

def test_negative_amount_rejected():
    try:
        is_high_amount(-1)
        assert False, "Expected ValueError"
    except ValueError:
        pass
