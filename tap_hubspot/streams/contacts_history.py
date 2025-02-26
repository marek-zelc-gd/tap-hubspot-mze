from typing import IO, Any, Dict, Optional,Iterable
import json
from singer_sdk import typing as th  # JSON Schema typing helpers
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot.client import HubSpotStream


class ContactsHistoryStream(HubSpotStream):
    """ContactsHistory."""

    name = "contacts_history"
    search_path = "/crm/v3/objects/contacts"
    full_path = "/crm/v3/objects/contacts"
    properties_object_type = "contacts"
    primary_keys = ["id"]

    # Set if forcing non-search endpoint
    forced_get = True

    @property
    def rest_method(self) -> str:
      return "GET"

    @property
    def schema(self):
        props = th.PropertiesList(
            th.Property(
                "id",
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
                "propertiesWithHistory",
                th.StringType,
            ),
        )

        # Needs to be defined manually since 2023-07-12 when
        # the list of properties in hubspot became to long for a request.
        # Error: 414 Client Error: URI Too Long for path

        self.extra_properties = [
                           "hs_object_id"
        ]

        self.properties_with_history = [
                   "email",
                   "hs_analytics_last_url",
                   "hs_analytics_last_referrer"
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


    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
        ) -> Dict[str, Any]:
            """Return a dictionary of values to be used in URL parameterization."""
            if not self.forced_get and self.replication_method == REPLICATION_INCREMENTAL:
                # Handled in prepare_request_payload instead
                return {}

            params: dict = {
                # Hubspot sets a limit of most 100 per request. Default is 10
                "limit": self.config.get("limit", 50)
            }
            props_to_get = self.get_properties()
            if props_to_get:
                params["properties"] = props_to_get

                # přidáno
            props_with_history_to_get = self.get_properties_with_history()
            if props_with_history_to_get:
                params["propertiesWithHistory"] = props_with_history_to_get
                #

            if next_page_token:
                params["after"] = next_page_token

            self.logger.debug(f"UrlParams after: {next_page_token}")
            self.logger.debug(f"UUUUUUUUrl Params: {params}")
            return params

    def get_properties_with_history(self) -> Iterable[str]:
            """Override to return a list of properties to fetch for objects"""
            if self.properties_with_history is not None:
                return self.properties_with_history

            if not self.properties_object_type:
                self.properties_with_history = []
                return self.properties_with_history

            request = self.build_prepared_request(
                method="GET",
                url="".join(
                    [self.url_base, f"/crm/v3/properties/{self.properties_object_type}"]
                ),
                headers=self.http_headers,
            )

            session = self._requests_session or requests.Session()

            r = session.send(request)

            if r.status_code != 200:
                raise RuntimeError(f"Could not fetch properties: {r.status_code}, {r.text}")


            self.properties_with_history = []
            for p in extract_jsonpath("$.results[*]", input=r.json()):
                self.properties_with_history.append(p["name"])
            return self.properties_with_history


    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """As needed, append or transform raw data to match expected structure."""
        # Need to copy the replication key to top level so that meltano can read it
        if self.replication_key:
            row[self.replication_key] = self.get_replication_key_value(row)
        # Convert properties and associations back into JSON
        if "properties" in row:
            jsonprops = json.dumps(row.get("properties"))
            row["properties"] = jsonprops
        if "associations" in row:
            jsonassoc = json.dumps(row.get("associations"))
            row["associations"] = jsonassoc
        # Add logic to handle propertiesWithHistory
        if "propertiesWithHistory" in row:
            jsonprops_with_history = json.dumps(row.get("propertiesWithHistory"))
            row["propertiesWithHistory"] = jsonprops_with_history
        return row

    @property
    def replication_key(self) -> Optional[str]:
         return None if self.config.get("no_search", False) else "lastmodifieddate"

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



    def parse_response(self, response: requests.Response) -> Iterable[dict]:


        #Parse the response and return an iterator of result rows.
        self.logger.info(f"AAA DEBUG: Request URL: {response.request.url}")
        #self.logger.info(f"DEBUG: Request Method: {response.request.method}")
        #self.logger.info(f"DEBUG: Request Body: {response.request.body}")
        #self.logger.info(f"DEBUG: Request Params: {response.request.params}")
        #self.logger.info(f"DEBUG: Response headers: {response.headers}")
        #self.logger.info(f"DDDDDDDDDDDEBUG: Response JSON: {response.json()}")

        if response.status_code != 200:
            self.logger.error(f"EEError response: {response.text}")
            raise FatalAPIError(f"{response.status_code} Client Error: {response.reason} with path: {response.request.url} with params: {response.request.params}")


        try:
            yield from extract_jsonpath(self.records_jsonpath, input=response.json())
        except Exception as e:
            self.logger.error(f"Error parsing response: {e}")
            raise
