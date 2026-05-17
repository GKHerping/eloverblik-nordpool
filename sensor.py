from __future__ import annotations

from datetime import date

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.helpers.update_coordinator import CoordinatorEntity

DOMAIN = "eloverblik_nordpool"


async def async_setup_entry(hass, entry, async_add_entities) -> None:
    coordinator = hass.data[DOMAIN][entry.entry_id]
    mp_id = entry.data["metering_point_id"]

    async_add_entities(
        [
            CurrentAdjustedPriceSensor(coordinator, entry.entry_id, mp_id),
            CurrentGridTariffSensor(coordinator, entry.entry_id, mp_id),
            TodayPricesSensor(coordinator, entry.entry_id, mp_id),
            TomorrowPricesSensor(coordinator, entry.entry_id, mp_id),
            HourlyConsumptionSensor(coordinator, entry.entry_id, mp_id),
            ConsumptionWatermarkSensor(coordinator, entry.entry_id, mp_id),
            CostWatermarkSensor(coordinator, entry.entry_id, mp_id),
        ]
    )


# ---------------------------------------------------------------------------
# Shared base
# ---------------------------------------------------------------------------

class _EloverblikSensor(CoordinatorEntity, SensorEntity):

    def __init__(
        self,
        coordinator,
        entry_id: str,
        metering_point_id: str,
        unique_suffix: str,
        name: str,
    ) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{entry_id}_{unique_suffix}"
        self._attr_name = name
        self._attr_has_entity_name = True
        self._metering_point_id = metering_point_id

    @property
    def device_info(self) -> dict:
        return {
            "identifiers": {(DOMAIN, self._metering_point_id)},
            "name": f"Eloverblik (…{self._metering_point_id[-4:]})",
            "manufacturer": "Glenn Herping",
            "model": "Track your consumption",
        }


# ---------------------------------------------------------------------------
# Current adjusted price
# ---------------------------------------------------------------------------

class CurrentAdjustedPriceSensor(_EloverblikSensor):
    """
    Full consumer price for the current hour in DKK/kWh:
        (spot + fortjeneste + grid_tariff[hour]) × VAT

    NOTE: Used for live display only. For Energy Dashboard cost tracking,
    use the external statistic  eloverblik_nordpool:total_cost_…  instead
    (select 'Use an entity tracking the total costs').
    """

    _attr_native_unit_of_measurement = "DKK/kWh"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_icon = "mdi:cash-clock"
    _attr_suggested_display_precision = 4

    def __init__(self, coordinator, entry_id, mp_id) -> None:
        super().__init__(coordinator, entry_id, mp_id,
                         "current_adjusted_price", "Current Adjusted Price")

    @property
    def native_value(self) -> float | None:
        return self.coordinator.data.get("current_adjusted_price")

    @property
    def extra_state_attributes(self) -> dict:
        d = self.coordinator.data
        return {
            "current_hour": d.get("current_hour"),
            "current_grid_tariff_dkk_kwh": d.get("current_grid_tariff"),
            "today_hourly": d.get("today_prices", []),
            "tomorrow_hourly": d.get("tomorrow_prices", []),
        }


# ---------------------------------------------------------------------------
# Current grid tariff
# ---------------------------------------------------------------------------

class CurrentGridTariffSensor(_EloverblikSensor):
    """Sum of active nettarif + afgifter for the current hour (DKK/kWh)."""

    _attr_native_unit_of_measurement = "DKK/kWh"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_icon = "mdi:transmission-tower"
    _attr_suggested_display_precision = 4

    def __init__(self, coordinator, entry_id, mp_id) -> None:
        super().__init__(coordinator, entry_id, mp_id,
                         "current_grid_tariff", "Current Grid Tariff")

    @property
    def native_value(self) -> float | None:
        return self.coordinator.data.get("current_grid_tariff")

    @property
    def extra_state_attributes(self) -> dict:
        return {"hourly": self.coordinator.data.get("hourly_grid_tariff", [])}


# ---------------------------------------------------------------------------
# Today's 24 adjusted prices
# ---------------------------------------------------------------------------

class TodayPricesSensor(_EloverblikSensor):
    """State = current hour's adjusted price. Attribute hourly = all 24 values."""

    _attr_native_unit_of_measurement = "DKK/kWh"
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_icon = "mdi:sun-clock"
    _attr_suggested_display_precision = 4

    def __init__(self, coordinator, entry_id, mp_id) -> None:
        super().__init__(coordinator, entry_id, mp_id,
                         "today_prices", "Today Adjusted Prices")

    @property
    def native_value(self) -> float | None:
        prices = self.coordinator.data.get("today_prices", [])
        hour = self.coordinator.data.get("current_hour", 0)
        if prices and hour < len(prices):
            return prices[hour]
        return None

    @property
    def extra_state_attributes(self) -> dict:
        return {"hourly": self.coordinator.data.get("today_prices", [])}


# ---------------------------------------------------------------------------
# Tomorrow's 24 adjusted prices
# ---------------------------------------------------------------------------

class TomorrowPricesSensor(_EloverblikSensor):
    """State = number of published tomorrow prices (0 until ~13:00 CET)."""

    _attr_native_unit_of_measurement = "h"
    _attr_icon = "mdi:weather-sunset-up"

    def __init__(self, coordinator, entry_id, mp_id) -> None:
        super().__init__(coordinator, entry_id, mp_id,
                         "tomorrow_prices", "Tomorrow Adjusted Prices")

    @property
    def native_value(self) -> int:
        prices = self.coordinator.data.get("tomorrow_prices", [])
        return sum(1 for p in prices if p is not None)

    @property
    def extra_state_attributes(self) -> dict:
        prices = self.coordinator.data.get("tomorrow_prices", [])
        return {
            "hourly": prices,
            "available": any(p is not None for p in prices),
        }


# ---------------------------------------------------------------------------
# Hourly consumption (InfluxDB bridge)
# ---------------------------------------------------------------------------

class HourlyConsumptionSensor(_EloverblikSensor):
    """
    State = current hour's consumption in kWh.

    The coordinator updates every hour, so InfluxDB receives one data point
    per hour.  This builds a time series that Grafana can query like:
        import "date"
        from(bucket: "home_assistant")
          |> range(start: -90d)
          |> filter(fn: (r) => r["entity_id"] == "sensor.eloverblik_hourly_consumption")
          |> filter(fn: (r) => r["_field"] == "value")
          |> map(fn: (r) => ({r with hour_of_day: date.hour(t: r._time)}))
          |> group(columns: ["hour_of_day"])
          |> mean(column: "_value")

    The 'hourly' attribute holds all 24 values for today (convenient for
    Lovelace cards), but InfluxDB only indexes the state value.
    """

    _attr_native_unit_of_measurement = "kWh"
    _attr_state_class = SensorStateClass.MEASUREMENT   # not TOTAL — each hour
    _attr_icon = "mdi:lightning-bolt-circle"            # is a standalone reading
    _attr_suggested_display_precision = 3

    def __init__(self, coordinator, entry_id, mp_id) -> None:
        super().__init__(
            coordinator, entry_id, mp_id,
            "hourly_consumption", "Hourly Consumption"
        )

    @property
    def native_value(self) -> float | None:
        hourly = self.coordinator.data.get("today_hourly_consumption", [])
        hour   = self.coordinator.data.get("current_hour", 0)
        if hourly and hour < len(hourly):
            return hourly[hour]
        return None

    @property
    def extra_state_attributes(self) -> dict:
        return {
            "hourly": self.coordinator.data.get("today_hourly_consumption", []),
            "current_hour": self.coordinator.data.get("current_hour"),
        }
        
# ---------------------------------------------------------------------------
# Consumption watermark
# ---------------------------------------------------------------------------

class ConsumptionWatermarkSensor(_EloverblikSensor):
    """
    Date up to which consumption statistics have been fetched from Eloverblik.

    Energy Dashboard wiring
    ───────────────────────
    Settings → Energy → Grid consumption → Add consumption
      Source:  eloverblik_nordpool:consumption_<metering_point_id>
      Cost:    Use an entity tracking the total costs
               → select  eloverblik_nordpool:total_cost_<metering_point_id>
    """

    _attr_device_class = SensorDeviceClass.DATE
    _attr_icon = "mdi:database-clock"

    def __init__(self, coordinator, entry_id, mp_id) -> None:
        super().__init__(coordinator, entry_id, mp_id,
                         "consumption_watermark", "Consumption Data Up To")
        self._mp_id = mp_id

    @property
    def native_value(self):
        wm = self.coordinator.data.get("consumption_watermark")
        if wm:
            try:
                return date.fromisoformat(wm)
            except ValueError:
                return None
        return None

    @property
    def extra_state_attributes(self) -> dict:
        return {
            "statistic_id": self.coordinator.data.get("stat_id_consumption", ""),
        }


# ---------------------------------------------------------------------------
# Cost watermark
# ---------------------------------------------------------------------------

class CostWatermarkSensor(_EloverblikSensor):
    """
    Date up to which cost statistics (DKK) have been computed and stored.

    The underlying external statistic  eloverblik_nordpool:total_cost_…
    contains the hourly DKK cost = consumption_kWh × adjusted_price.
    It is the correct entity to select under
    'Use an entity tracking the total costs' in the Energy Dashboard —
    HA will use the cumulative sum to display accurate historical costs
    even for dates before this integration was installed.

    Nord Pool's API provides ~60 days of historical prices, so costs are
    backfilled up to that limit automatically on first run.
    """

    _attr_device_class = SensorDeviceClass.DATE
    _attr_icon = "mdi:cash-check"

    def __init__(self, coordinator, entry_id, mp_id) -> None:
        super().__init__(coordinator, entry_id, mp_id,
                         "cost_watermark", "Cost Data Up To")
        self._mp_id = mp_id

    @property
    def native_value(self):
        wm = self.coordinator.data.get("cost_watermark")
        if wm:
            try:
                return date.fromisoformat(wm)
            except ValueError:
                return None
        return None

    @property
    def extra_state_attributes(self) -> dict:
        return {
            "statistic_id": self.coordinator.data.get("stat_id_cost", ""),
        }
