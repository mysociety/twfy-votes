from twfy_votes.apps.decisions.motion_analysis import VoteType, catagorise_motion


class BaseMotionTest:
    motions: list[str] = []
    expected: VoteType

    def test_motion(self):
        for motion in self.motions:
            assert (
                catagorise_motion(motion) == self.expected
            ), f"Motion {motion} should be {self.expected}"


class Test10Minute(BaseMotionTest):
    motions = ["Question put (Standing Order No. 23)."]
    expected = VoteType.TEN_MINUTE_RULE


class TestInstrument(BaseMotionTest):
    motions = [
        "That the draft Postal Packets (Miscellaneous Amendments) Regulations 2023, which were laid before this House on 29 June, be approved.—(Mike Wood.)"
    ]
    expected = VoteType.APPROVE_STATUTORY_INSTRUMENT


class TestHumbleAddress(BaseMotionTest):
    motions = [
        """
               That an humble Address be presented to His Majesty, that he will be graciously pleased to give directions that the Secretary of State for Levelling Up, Housing and Communities provide all papers, advice and correspondence involving Ministers, senior officials and special advisers, including submissions and electronic communications, relating to the decision by the Secretary of State for Levelling Up, Housing and Communities and the Prime Minister to commission a review into the Tees Valley Combined Authority’s oversight of the South Tees Development Corporation and the Teesworks joint venture, including papers relating to the decision that this review should not be led by the National Audit Office.
                """
    ]
    expected = VoteType.HUMBLE_ADDRESS


class TestLordsAmendment(BaseMotionTest):
    motions = [
        """
        Motion made, and Question put, That this House disagrees with Lords amendment 23.—(Robert Jenrick.)
        """
    ]
    expected = VoteType.LORDS_AMENDMENT


class TestStandingOrderChange(BaseMotionTest):
    motions = [
        """
        Motion made, and Question proposed,

        That it be an instruction to the Select Committee to which the Holocaust Memorial Bill is committed to deal with the Bill as follows:

        (1) That the Committee treats the principle of the Bill, as determined by the House on the Bill’s Second Reading, as comprising the matters mentioned in paragraph 2; and those matters shall accordingly not be at issue during proceedings of the Committee.

        (2) The matters referred to in paragraph (1) are—

        (a) the Secretary of State may incur expenditure for or in connection with (i) a memorial commemorating the victims of the Holocaust, and (ii) a centre for learning relating to the memorial; and

        (b) section 8(1) and (8) of the London County Council (Improvements) Act 1900 are not to prevent, restrict or otherwise affect the construction, use, operation, maintenance or improvement of such a memorial and centre for learning at Victoria Tower Gardens in the City of Westminster.

        (3) Given paragraph (2) and as the Bill does not remove the need for planning permission and all other necessary consents being obtained in the usual way for the construction, use, operation, maintenance and improvement of the memorial and centre for learning, the Committee shall not hear any petition against the Bill to the extent that the petition relates to—

        (a) the question of whether or not there should be a memorial commemorating the victims of the Holocaust or a centre for learning relating to the memorial, whether at Victoria Tower Gardens or elsewhere; or

        (b) whether or not planning permission and all other necessary consents should be given for the memorial and centre for learning, or the terms and conditions on which they should be given.

        (4) The Committee shall have power to consider any amendments proposed by the member in charge of the Bill which, if the Bill were a private bill, could not be made except upon petition for additional provision.

        (5) Paragraph (4) applies only so far as the amendments proposed by the member in charge of the Bill fall within the principle of the Bill as provided for by paragraphs (1) and (2) above.

        That these Orders be Standing Orders of the House.—(Felicity Buchan.)

        Amendment proposed: (a), to leave out from “memorial” in paragraph (2)(a) to the end of paragraph (2)(b).—(Sir Peter Bottomley.)

        Question put, That the amendment be made.
        """
    ]
    expected = VoteType.STANDING_ORDER_CHANGE


class TestThirdReading(BaseMotionTest):
    motions = [
        """
        I beg to move, That the Bill be now read the Third time.        """
    ]
    expected = VoteType.THIRD_STAGE


class TestSecondCommittee(BaseMotionTest):
    motions = [
        """
            Question put, That the clause be read a Second time.
        """
    ]
    expected = VoteType.SECOND_STAGE_COMMITTEE


class TestAmendment(BaseMotionTest):
    motions = [
        """
            Question put, That the amendment be made.

        """
    ]
    expected = VoteType.AMENDMENT
