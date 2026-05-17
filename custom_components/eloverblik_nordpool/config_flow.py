from __future__ import annotations

import logging

import aiohttp
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.selector import NumberSelector, TextSelector

DOMAIN = "eloverblik_nordpool"
ELOVERBLIK_BASE = "https://api.eloverblik.dk/customerapi/api"
DEFAULT_VAT = 25.0

_LOGGER = logging.getLogger(__name__)


def _build_schema(defaults: dict | None = None) -> vol.Schema:
    """
    Build the config schema lazily inside the step method.

    Selectors are constructed here rather than at module level so that any
    selector instantiation error surfaces as a visible HA log entry rather
    than silently aborting the module import and producing a blank form.

    NumberSelector only receives 'min' and 'step' — HA selector kwargs vary
    across releases and extra keys (mode, unit_of_measurement) can throw.
    """
    d = defaults or {}
    return vol.Schema(
        {
            vol.Required(
                "refresh_token",
                default=d.get("refresh_token", ""),
            ): TextSelector({"type": "password"}),

            vol.Required(
                "metering_point_id",
                default=d.get("metering_point_id", ""),
            ): TextSelector(),

            # The Nord Pool config entry ID is a 26-character alphanumeric string.
            # Find it in Home Assistant via:
            #   Developer Tools → Actions → nordpool.get_prices_for_date → YAML view
            # The value next to "config_entry:" is what to paste here.
            vol.Required(
                "nordpool_config_entry_id",
                default=d.get("nordpool_config_entry_id", ""),
            ): TextSelector(),

            vol.Optional(
                "fortjeneste",
                default=d.get("fortjeneste", 0.0),
            ): NumberSelector({"min": 0.0, "step": 0.001}),

            vol.Optional(
                "vat_percent",
                default=d.get("vat_percent", DEFAULT_VAT),
            ): NumberSelector({"min": 0.0, "max": 100.0, "step": 0.1}),
        }
    )


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the config flow for Eloverblik Nordpool."""

    VERSION = 1

    async def async_step_user(self, user_input: dict | None = None) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            # Prevent the same metering point from being added twice
            await self.async_set_unique_id(user_input["metering_point_id"])
            self._abort_if_unique_id_configured()

            try:
                async with aiohttp.ClientSession() as session:
                    # 1. Exchange refresh token for short-lived access token
                    headers = {
                        "Authorization": f"Bearer {user_input['refresh_token']}"
                    }
                    async with session.get(
                        f"{ELOVERBLIK_BASE}/token", headers=headers
                    ) as resp:
                        if resp.status == 401:
                            errors["refresh_token"] = "invalid_auth"
                        else:
                            resp.raise_for_status()
                            token_data = await resp.json()
                            access_token = token_data["result"]

                    if not errors:
                        # 2. Confirm metering point belongs to this customer
                        headers["Authorization"] = f"Bearer {access_token}"
                        async with session.get(
                            f"{ELOVERBLIK_BASE}/meteringpoints/meteringpoints",
                            headers=headers,
                        ) as resp:
                            resp.raise_for_status()
                            mp_data = await resp.json()
                            valid_ids = [
                                p["meteringPointId"]
                                for p in mp_data.get("result", [])
                            ]
                            if user_input["metering_point_id"] not in valid_ids:
                                errors["metering_point_id"] = "invalid_metering_point"

                if not errors:
                    # 3. Confirm the Nord Pool config entry ID resolves
                    nordpool_entry = self.hass.config_entries.async_get_entry(
                        user_input["nordpool_config_entry_id"]
                    )
                    if nordpool_entry is None:
                        errors["nordpool_config_entry_id"] = "nordpool_entry_not_found"
                    elif not nordpool_entry.data.get("areas"):
                        errors["nordpool_config_entry_id"] = "nordpool_no_areas"

            except aiohttp.ClientResponseError as err:
                _LOGGER.exception("Eloverblik API error during config flow: %s", err)
                errors["base"] = "cannot_connect"
            except aiohttp.ClientError as err:
                _LOGGER.exception("Network error during config flow: %s", err)
                errors["base"] = "cannot_connect"
            except Exception as err:  # noqa: BLE001
                _LOGGER.exception("Unexpected error during config flow: %s", err)
                errors["base"] = "unknown"

            if not errors:
                metering_id = user_input["metering_point_id"]
                return self.async_create_entry(
                    title=f"Eloverblik (…{metering_id[-4:]})",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user",
            data_schema=_build_schema(user_input),
            errors=errors,
        )