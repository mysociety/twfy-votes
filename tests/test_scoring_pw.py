from math import isclose

from twfy_votes.apps.policies.scoring import PublicWhipScore, ScoreFloatPair


def test_abstain():
    result = PublicWhipScore.score(
        votes_same=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_different=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_absent=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_abstain=ScoreFloatPair(weak=0.0, strong=1.0),
        agreements_same=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_different=ScoreFloatPair(weak=0.0, strong=0.0),
    )

    assert result == 0.5, "Expected score to be 0.5 when all votes are abstain"


def test_midway_score():
    result = PublicWhipScore.score(
        votes_same=ScoreFloatPair(weak=0.0, strong=5.0),
        votes_different=ScoreFloatPair(weak=0.0, strong=5.0),
        votes_absent=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_abstain=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_same=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_different=ScoreFloatPair(weak=0.0, strong=0.0),
    )

    assert (
        result == 0.5
    ), "Expected score to be 0.5 when half of the votes are the same and half are different"


def test_similar_score():
    result = PublicWhipScore.score(
        votes_same=ScoreFloatPair(weak=0.0, strong=15.0),
        votes_different=ScoreFloatPair(weak=0.0, strong=5.0),
        votes_absent=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_abstain=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_same=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_different=ScoreFloatPair(weak=0.0, strong=0.0),
    )

    assert (
        result == 0.25
    ), "Expected score to be 0.25 when a quarter of the votes are different and three quarters are the same"


def test_relative_weak_vote():
    # weak votes worth 10 points, strong votes worth 50 points

    result = PublicWhipScore.score(
        votes_same=ScoreFloatPair(weak=1.0, strong=0.0),
        votes_different=ScoreFloatPair(weak=0.0, strong=1.0),
        votes_absent=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_abstain=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_same=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_different=ScoreFloatPair(weak=0.0, strong=0.0),
    )

    # when there is one different strong vote, and one different weak vote
    # the total avaliable agreement score is 60
    # and the score should be 50/60

    assert isclose(result, 50 / 60), f"Expected score to be 50/60 but got {result}"


def test_weird_absent_rule():
    result = PublicWhipScore.score(
        votes_same=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_different=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_absent=ScoreFloatPair(weak=1.0, strong=1.0),
        votes_abstain=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_same=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_different=ScoreFloatPair(weak=0.0, strong=0.0),
    )

    # strong absences worth 25/50, weak absences worth 1/2
    # so the score should be 26/52

    assert isclose(result, 26 / 52), f"Expected score to be 26/52 but got {result}"


def test_absences_are_abstains():
    # these are grouped for this test
    result = PublicWhipScore.score(
        votes_same=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_different=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_absent=ScoreFloatPair(weak=0.0, strong=0.0),
        votes_abstain=ScoreFloatPair(weak=1.0, strong=1.0),
        agreements_same=ScoreFloatPair(weak=0.0, strong=0.0),
        agreements_different=ScoreFloatPair(weak=0.0, strong=0.0),
    )

    # strong absences worth 25/50, weak absences worth 1/2
    # so the score should be 26/52

    assert isclose(result, 26 / 52), f"Expected score to be 26/52 but got {result}"
