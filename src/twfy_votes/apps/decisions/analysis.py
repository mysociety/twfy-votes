def is_nonaction_vote(motion_text: str, quiet: bool = True) -> bool:
    """
    Analyse the text of a motion to determine if it is a non-action motion
    """
    non_action_phrases = [
        "believes",
        "regrets",
        "notes with approval",
        "expressed approval",
        "welcomes",
        "is concerned",
        "calls on the",
        "recognises",
        "takes note",
        "agrees with the goverment's decision",
    ]
    action_phrases = [
        "orders that",
        "requires the Goverment",
        "censures",
        "declines to give a Second Reading",
    ]

    # this doesn't seem like a thing
    # commits the Government

    reduced_text = motion_text.lower()

    score = 0
    for phrase in non_action_phrases:
        if phrase in reduced_text:
            if not quiet:
                print(f"matched {phrase}")
            score += 1

    for phrase in action_phrases:
        if phrase in reduced_text:
            if not quiet:
                print(f"matched {phrase}- is action")
            score = 0

    return score > 0
