from fastapi.testclient import TestClient


class BaseTestResponse:
    url: str = "/"
    status_code: int = 200
    must_contain: list[str] = []
    must_not_contain: list[str] = []
    has_json: bool = False

    def test_present(self, client: TestClient):
        response = client.get(self.url)
        assert response.status_code == self.status_code

        for item in self.must_contain:
            assert item in response.text, f"Missing {item}"

        for item in self.must_not_contain:
            assert item not in response.text, f"Unexpected {item}"

    def test_json(self, client: TestClient):
        if not self.has_json:
            return
        response = client.get(self.url + ".json")
        assert response.status_code == self.status_code
        assert response.json() is not None


class TestIndex(BaseTestResponse):
    url = "/"


class TestPersons(BaseTestResponse):
    url = "/persons"
    status_code = 404


class TestCurrentPeople(BaseTestResponse):
    url = "/people/current"
    has_json = True


class TestAllPeople(BaseTestResponse):
    url = "/people/all"
    has_json = True


class TestPerson(BaseTestResponse):
    url = "/person/10001"
    has_json = True


class TestPolicies(BaseTestResponse):
    url = "/policies"
    has_json = True


class TestActiveCommonsPolicies(BaseTestResponse):
    url = "/policies/commons/active/all"
    has_json = True


class TestCandidateCommonsPolicies(BaseTestResponse):
    url = "/policies/commons/candidate/all"
    has_json = True


class TestPolicy(BaseTestResponse):
    url = "/policy/6679"
    has_json = True


class TestDecisions(BaseTestResponse):
    url = "/decisions"
    has_json = True


class TestDivision(BaseTestResponse):
    url = "/decisions/division/commons/2023-10-17/328"
    has_json = True


class TestDivisionsYear(BaseTestResponse):
    url = "/decisions/commons/2023"
    has_json = True


class TestDivisionsMonth(BaseTestResponse):
    url = "/decisions/commons/2023/10"
    has_json = True


class TestPersonPolicy(BaseTestResponse):
    url = "/person/10001/records/commons/labour"
    has_json = True


class TestAgreementInfo(BaseTestResponse):
    url = "/decisions/agreement/commons/2019-06-24/b.530.1"
    has_json = True


def test_valid_policy_xml(client: TestClient):
    response = client.get("/twfy-compatible/policies/6679.xml")
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/xml"


def test_vote_popolo(client: TestClient):
    response = client.get("/twfy-compatible/popolo/6679.json")
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"


def test_vote_participants_2005(client: TestClient):
    response = client.get("/decisions/division/commons/2005-11-22/105.json")
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    data = response.json()

    assert data["overall_breakdown"]["total_possible_members"] == 646


def test_vote_participants_2015(client: TestClient):
    response = client.get("/decisions/division/commons/2015-12-08/145.json")
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    data = response.json()

    assert data["overall_breakdown"]["total_possible_members"] == 650


def test_all_popolo_policies(client: TestClient):
    """
    Check all popolo policies are valid for the commons active list.
    This one takes a bit of time, but catches all errors that would stump the
    TheyWorkForYou importer.
    Given this is the most important bridging function.
    """
    response = client.get("/policies/commons/active/all.json")
    data = response.json()

    policy_ids = [policy["id"] for policy in data["policies"]]
    for p in policy_ids:
        response = client.get(f"/twfy-compatible/popolo/{p}.json")
        assert response.status_code == 200, f"Failed on {p}"
