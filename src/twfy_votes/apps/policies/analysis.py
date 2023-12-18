def public_whip_score_difference(
    *,
    num_votes_same: float,
    num_strong_votes_same: float,
    num_votes_different: float,
    num_strong_votes_different: float,
    num_votes_absent: float,
    num_strong_votes_absent: float,
    num_strong_votes_abstain: float,
    num_votes_abstain: float,
) -> float:
    """
    Calculate the classic Public Whip score for a difference between two MPs.

    The score is a number between 0 and 1, where 0 is a perfect match and 1 is a perfect
    mismatch. Returns None if there are no votes to compare.

    This assumes two kinds of votes: normal and strong.

    Normal votes are worth a base score of 10/10 points if aligned, 0/10 points if not aligned, and 1/2 points if absent.
    Strong votes are worth a base score of 50/50 points if aligned, 0/50 points if not aligned, and 25/50 points if absent.

    The weird bit of complexity here is absences on normal votes reduce the total of the comparison.
    This means that MPs are only lightly penalised for missing votes if they attended some votes, or if there are strong votes.
    If all votes are normal and absent, the score will be 0.5.

    So if there were five normal votes, two in line with the policy, and three absent - the difference would be 0.12.
    But if normal votes were treated the same way as strong votes (5/10) - the difference would be 0.3.

    So the practical result of making a policy a mix of strong and normal votes is first,
    obviously that normal votes make up a smaller part of the total score.
    But the second is that strong votes penalise absences more than normal votes.

    Strong votes were originally intended to reflect three line whips, but in practice have broadened out to mean 'more important'.

    """

    # treat abstentions as absences
    num_votes_absent_or_abstain = num_votes_absent + num_votes_abstain
    num_strong_votes_absent_or_abstain = (
        num_strong_votes_absent + num_strong_votes_abstain
    )

    normal_weight = 10.0
    strong_weight = 50.0
    absence_weight = 1.0
    absence_total_weight = 2.0

    points = (
        normal_weight * num_votes_different
        + strong_weight * num_strong_votes_different
        + absence_weight * num_votes_absent_or_abstain
        + (
            (strong_weight / 2) * num_strong_votes_absent_or_abstain
        )  # Absences on strong votes are worth half the strong value
    )

    avaliable_points = (
        normal_weight * num_votes_same
        + normal_weight * num_votes_different
        + strong_weight * num_strong_votes_same
        + strong_weight * num_strong_votes_different
        + strong_weight * num_strong_votes_absent_or_abstain
        + (
            absence_total_weight * num_votes_absent_or_abstain
        )  # Absences on normal votes reduce the total of the comparison
    )

    if avaliable_points == 0:
        return -1

    return points / avaliable_points


def simplified_score_difference(
    *,
    num_votes_same: float,
    num_strong_votes_same: float,
    num_votes_different: float,
    num_strong_votes_different: float,
    num_votes_absent: float,
    num_strong_votes_absent: float,
    num_strong_votes_abstain: float,
    num_votes_abstain: float,
) -> float:
    """
    This is a simplified version of the public whip scoring system.
    Normal weight votes are 'informative' only, and have no score.
    neither has an absence weight.
    Abstensions are recorded as present - but half the value of a normal vote.

    """

    normal_weight = 0.0  # worth no points
    strong_weight = 10.0
    abstain_weight = normal_weight / 2  # half marks
    strong_abstain_weight = strong_weight / 2  # half marks
    absence_weight = 0.0  # absences are no points
    absence_total_weight = 0.0  # out of no points
    strong_absence_weight = 0.0  # absences are no points
    strong_absence_total_weight = 0.0  # out of no points

    points = (
        normal_weight * num_votes_different
        + strong_weight * num_strong_votes_different
        + absence_weight * num_votes_absent
        + strong_absence_weight * num_strong_votes_absent
        + abstain_weight * num_votes_abstain
        + strong_abstain_weight * num_strong_votes_abstain
    )

    avaliable_points = (
        normal_weight * num_votes_same
        + normal_weight * num_votes_different
        + strong_weight * num_strong_votes_same
        + strong_weight * num_strong_votes_different
        + absence_total_weight * num_votes_absent
        + strong_absence_total_weight * num_strong_votes_absent
        + abstain_weight * num_votes_abstain
        + strong_abstain_weight * num_strong_votes_abstain
    )

    return points / avaliable_points
