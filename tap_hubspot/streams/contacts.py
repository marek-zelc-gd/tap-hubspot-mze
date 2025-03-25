from typing import Optional

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_hubspot.client import HubSpotStream


class ContactsStream(HubSpotStream):
    """Contacts."""

    name = "contacts"
    search_path = "/crm/v3/objects/contacts/search"
    full_path = "/crm/v3/objects/contacts"
    properties_object_type = "contacts"
    primary_keys = ["id"]

    @property
    def schema(self):
        props = th.PropertiesList(
            th.Property(
                "id",
                th.StringType,
            ),
            th.Property(
                "properties",
                th.StringType,
            ),
            th.Property(
                "createdAt",
                th.DateTimeType,
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
                "archivedAt",
                th.DateTimeType,
            ),
            th.Property(
                "associations",
                th.StringType,
            ),
        )

        # Needs to be defined manually since 2023-07-12 when
        # the list of properties in hubspot became to long for a request.
        # Error: 414 Client Error: URI Too Long for path
        self.extra_properties = [
                   "hs_object_id", 
                   "email", 
                   "firstname", 
                   "lastname", 
                   "associatedcompanyid", 
                   "hs_date_entered_lead", 
                   "first_conversion_event_name", 
                   "leadstatus", 
                   "contact_status__c", 
                   "salesforceleadid", 
                   "salesforcecontactid", 
                   "salesforceaccountid", 
                   "salesforceownerid", 
                   "salesforcecampaignids", 
                   "product_trial_registration_date__c", 
                   "product_trial_status__c", 
                   "mql_score__c", 
                   "mqled_on__c", 
                   "original_mql_date__c", 
                   "lead_rating__c", 
                   "lead_rating_del__c", 
                   "campaign_attribution_field___new", 
                   "hs_analytics_source", 
                   "hs_analytics_source_data_1", 
                   "hs_analytics_source_data_2", 
                   "keyword__c", 
                   "hs_analytics_first_url", 
                   "hs_analytics_last_url", 
                   "hs_analytics_first_referrer", 
                   "hs_analytics_last_referrer", 
                   "hasoptedintoemail__c", 
                   "hs_email_optout", 
                   "lead_source_original___new", 
                   "hs_email_domain", 
                   "country",
                   "auth0_email_verification",
                   "to_be_deleted__c",
                   "gacid",
                   "hs_google_click_id",
                   "hs_last_sales_activity_timestamp",
                   "form_gated_assets_url",
                   "hs_latest_source",
                   "hs_latest_source_data_1",
                   "hs_latest_source_data_2",
                   "hs_latest_source_timestamp",
                   "hs_analytics_first_visit_timestamp",
                   "hs_analytics_last_visit_timestamp",
                   "hs_analytics_first_timestamp",
                   "hs_analytics_last_timestamp",
                   "data_sources_form_field"
        ]

        if self.replication_key:
            props.append(
                th.Property(
                    self.replication_key,
                    th.DateTimeType,
                )
            )

        return props.to_dict()

    @property
    def is_sorted(self) -> bool:
        """Check if stream is sorted.

        When `True`, incremental streams will attempt to resume if unexpectedly
        interrupted.

        This setting enables additional checks which may trigger
        `InvalidStreamSortException` if records are found which are unsorted.

        Returns:
            `True` if stream is sorted. Defaults to `False`.
         """

        return False
    
    @property
    def replication_key(self) -> Optional[str]:
        return None if self.config.get("no_search", False) else "hs_lastmodifieddate"

    @replication_key.setter
    def replication_key(self, _):
        "Just to shut Lint up"
        pass

    @property
    def path(self) -> str:
        return (
            self.full_path if self.config.get("no_search", False) else self.search_path
        )

    @path.setter
    def path(self, _):
        "Just to shut Lint up"
        pass
