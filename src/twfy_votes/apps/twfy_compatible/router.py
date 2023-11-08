# Views

import xml.etree.ElementTree as ET
from xml.dom import minidom

from fastapi import Response
from twfy_votes.apps.policies.models import PersonPolicyLink

from ...helpers.static_fastapi.static import StaticAPIRouter
from ...internal.settings import settings
from ..core.dependencies import GetContext

router = StaticAPIRouter(template_directory=settings.template_dir)


@router.get("/twfy-compatible/policies/{policy_id}.xml")
async def twfy_compatible_xml_policy(context: GetContext, policy_id: int):
    """
    Generate a TheyWorkForYou compatible XML file for a policy
    """
    policy_links = await PersonPolicyLink.from_policy_id(policy_id)
    root = ET.Element("publicwhip")
    for person in policy_links:
        personinfo = ET.SubElement(root, "personinfo")
        for key, value in person.xml_dict().items():
            personinfo.set(key, value)

    str_xml = ET.tostring(root, "utf-8")
    pretty = minidom.parseString(str_xml).toprettyxml(indent="  ")

    return Response(content=pretty, media_type="application/xml")
