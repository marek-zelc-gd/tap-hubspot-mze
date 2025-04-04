from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_hubspot.client import HubSpotStream


class TicketsAssociationsStream(HubSpotStream):
    """Ticket's associations."""

    def get_properties(self):
        return []

    name = "tickets_associations"
    path = "/crm/v4/objects/tickets/?associations=companies,contacts"
    properties_object_type = "tickets"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
        ),
        th.Property(
            "updatedAt",
            th.DateTimeType,
        ),
        th.Property(
            "archived",
            th.BooleanType,
        ),
        th.Property(
            "associations",
            th.StringType,
        ),
    ).to_dict()
