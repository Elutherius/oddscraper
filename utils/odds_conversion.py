def prob_to_moneyline(p):
    """Converts a probability (0-1) to American Moneyline odds."""
    if p <= 0 or p >= 1:
        return None 
    if p == 0.5:
        return 100 
    if p > 0.5:
        ml = -(p / (1 - p)) * 100
    else:
        ml = ((1 - p) / p) * 100
    return int(round(ml))

def moneyline_to_prob(ml):
    """Converts American Moneyline odds to implied probability (0-1)."""
    if ml == 0:
        return None
    if ml > 0:
        return 100 / (ml + 100)
    else:
        return (-ml) / ((-ml) + 100)
