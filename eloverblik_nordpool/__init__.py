from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
import zoneinfo

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData, StatisticMeanType
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
)

DOMAIN = "eloverblik_nordpool"
ELOVERBLIK_BASE = "https://api.eloverblik.dk/customerapi/api"

# Nord Pool public API allows ~2 months of historical prices
_NORDPOOL_HISTORY_DAYS = 60

# Eloverblik holds up to ~3 years; cap initial backfill to avoid runaway loops
_MAX_BACKFILL_DAYS = 3 * 365

# Chunk size for Eloverblik timeseries calls (days per request)
_FETCH_CHUNK_DAYS = 30

_DK_TZ = zoneinfo.ZoneInfo("Europe/Copenhagen")
_UTC = timezone.utc

_STORAGE_VERSION = 1

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Integration lifecycle
# ---------------------------------------------------------------------------

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    coordinator = EloverblikCoordinator(hass, entry)
    await coordinator.async_config_entry_first_refresh()
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    hass.data[DOMAIN].pop(entry.entry_id, None)
    return True


# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------

class EloverblikCoordinator(DataUpdateCoordinator):
    """
    Hourly coordinator that maintains two parallel external statistic series:

      1. eloverblik_nordpool:consumption_<mp_id>  [kWh, sum]
         Full history from Eloverblik (up to 3 years).

      2. eloverblik_nordpool:total_cost_<mp_id>   [DKK, sum]
         Historical cost = consumption × (spot + fortjeneste + tariff) × VAT
         Covered as far back as Nord Pool's API allows (~60 days).

    Both series use independent watermarks persisted in HA storage so that
    restarts, token expiry, and re-installs never create gaps or double-counts.

    Energy Dashboard wiring
    ───────────────────────
    Settings → Energy → Grid consumption → Add consumption
      • Source:  eloverblik_nordpool:consumption_…
      • Cost:    "Use an entity tracking the total costs"
                 → eloverblik_nordpool:total_cost_…
    """

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(hours=1),
        )
        self.config = entry.data
        mp_id = entry.data["metering_point_id"]
        self.stat_id_consumption = f"{DOMAIN}:consumption_{mp_id}"
        self.stat_id_cost        = f"{DOMAIN}:total_cost_{mp_id}"

        self._store: Store = Store(
            hass,
            _STORAGE_VERSION,
            f"{DOMAIN}_watermarks_{entry.entry_id}",
        )
        # Loaded lazily on first update; keys: "consumption", "cost"
        self._watermarks: dict = {}
        self._recent_hourly_consumption: list[float | None] = [None] * 24

    # ------------------------------------------------------------------
    # Eloverblik auth
    # ------------------------------------------------------------------

    async def _get_access_token(self, session: aiohttp.ClientSession) -> str:
        headers = {"Authorization": f"Bearer {self.config['refresh_token']}"}
        async with session.get(f"{ELOVERBLIK_BASE}/token", headers=headers) as resp:
            resp.raise_for_status()
            return (await resp.json())["result"]

    # ------------------------------------------------------------------
    # Date helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_date(val) -> date | None:
        if val is None:
            return None
        try:
            return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _date_range(from_str: str, to_str: str) -> list[str]:
        """Return every date string from from_str to to_str inclusive."""
        out = []
        cur = datetime.strptime(from_str, "%Y-%m-%d").date()
        end = datetime.strptime(to_str, "%Y-%m-%d").date()
        while cur <= end:
            out.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return out
    
    def _update_recent_hourly(self, pts: list[tuple[datetime, float]]) -> None:
        """
        Side-effect of backfill: capture the most recently available day's
        24-hour consumption so HourlyConsumptionSensor can expose it to
        InfluxDB without a separate API call.
        """
        if not pts:
            return
        latest_date = max(utc_ts.astimezone(_DK_TZ).date() for utc_ts, _ in pts)
        hourly: list[float | None] = [None] * 24
        for utc_ts, kwh in pts:
            local_dt = utc_ts.astimezone(_DK_TZ)
            if local_dt.date() == latest_date:
                hourly[local_dt.hour] = kwh
        if any(v is not None for v in hourly):
            self._recent_hourly_consumption = hourly

    # ------------------------------------------------------------------
    # Eloverblik: fetch one date-range chunk
    # Returns list of (utc_start, kwh) sorted ascending.
    # ------------------------------------------------------------------

    async def _fetch_consumption_chunk(
        self,
        session: aiohttp.ClientSession,
        headers: dict,
        from_date: str,
        to_date: str,
    ) -> list[tuple[datetime, float]]:
        mp_body = {
            "meteringPoints": {
                "meteringPoint": [self.config["metering_point_id"]]
            }
        }
        async with session.post(
            f"{ELOVERBLIK_BASE}/meterdata/gettimeseries"
            f"/{from_date}/{to_date}/Hour",
            headers=headers,
            json=mp_body,
        ) as resp:
            resp.raise_for_status()
            ts_data = await resp.json()

        if not isinstance(ts_data, dict):
            return []

        result = ts_data.get("result") or []
        if not result or not isinstance(result[0], dict):
            return []

        market_doc = result[0].get("MyEnergyData_MarketDocument") or {}
        time_series = market_doc.get("TimeSeries") or []
        if not time_series:
            return []

        points_out: list[tuple[datetime, float]] = []

        for ts in time_series:
            period = ts.get("Period", [])
            if isinstance(period, dict):
                period = [period]
            for p in period:
                interval_start = (
                    (p.get("timeInterval") or {}).get("start") or from_date
                )
                base_str = str(interval_start)[:10]
                pts = p.get("Point", [])
                if isinstance(pts, dict):
                    pts = [pts]
                for pt in pts:
                    try:
                        pos = int(pt.get("position", 0))
                        kwh = float(pt["out_Quantity.quantity"])
                        utc_start = (
                            datetime.strptime(base_str, "%Y-%m-%d")
                            .replace(tzinfo=_DK_TZ)
                            + timedelta(hours=pos - 1)
                        ).astimezone(_UTC)
                        points_out.append((utc_start, kwh))
                    except (KeyError, ValueError, TypeError):
                        continue

        return sorted(points_out, key=lambda x: x[0])
    
    # ------------------------------------------------------------------
    # Tariff helpers — date-aware so historical cost uses correct rates
    # ------------------------------------------------------------------

    def _is_active_on(self, charge: dict, target: date) -> bool:
        """Return True if this tariff/fee was valid on target date."""
        valid_from = self._to_date(charge.get("validFromDate"))
        valid_to   = self._to_date(charge.get("validToDate"))
        if valid_from and valid_from > target:
            return False
        if valid_to and valid_to < target:
            return False
        return True

    def _prices_to_hourly(self, prices: list[dict]) -> list[float] | None:
        if not prices:
            return None
        if len(prices) == 1:
            return [float(prices[0]["price"])] * 24
        hourly = [0.0] * 24
        if len(prices) == 24:
            for e in prices:
                pos = int(e["position"]) - 1
                hourly[pos] = float(e["price"])
            return hourly
        for e in prices:
            pos = int(e["position"]) - 1
            if 0 <= pos < 24:
                hourly[pos] = float(e["price"])
        last = 0.0
        for h in range(24):
            if hourly[h] != 0.0:
                last = hourly[h]
            else:
                hourly[h] = last
        return hourly

    def _parse_charges_for_date(
        self, charges_data: dict, target: date
    ) -> list[float]:
        """
        Return a 24-element list of grid charges (DKK/kWh) valid on target date.
        Includes both tariffs and fees (afgifter).
        """
        combined = [0.0] * 24
        try:
            result = charges_data["result"][0]["result"]
        except (KeyError, IndexError, TypeError):
            return combined
        for group in ("tariffs", "fees"):
            for charge in result.get(group, []):
                if not self._is_active_on(charge, target):
                    continue
                hourly = self._prices_to_hourly(charge.get("prices", []))
                if hourly is None:
                    continue
                for h in range(24):
                    combined[h] += hourly[h]
        return [round(v, 6) for v in combined]

    # today's tariffs (for live price sensor)
    def _parse_charges_today(self, charges_data: dict) -> list[float]:
        return self._parse_charges_for_date(
            charges_data, datetime.now(_DK_TZ).date()
        )

    # ------------------------------------------------------------------
    # Nord Pool price fetch (hourly, via action API)
    # ------------------------------------------------------------------

    def _nordpool_area(self) -> str:
        entry = self.hass.config_entries.async_get_entry(
            self.config["nordpool_config_entry_id"]
        )
        if not entry:
            raise UpdateFailed("Linked Nord Pool config entry not found")
        areas = entry.data.get("areas", [])
        if not areas:
            raise UpdateFailed("Nord Pool config entry has no areas configured")
        return areas[0]
    
    def _get_nordpool_tz(self) -> zoneinfo.ZoneInfo:
        """Return timezone matching the configured Nord Pool area."""
        area = self._nordpool_area().upper()
        if area in ("FI", "EE", "LV", "LT"):
            return zoneinfo.ZoneInfo("Europe/Helsinki")
        # DK/NO/SE/others use CET/CEST
        return _DK_TZ
        
    async def _fetch_nordpool_prices(
        self, date_str: str, area: str
    ) -> list[float | None]:
        """
        Fetch spot prices for one date via nordpool.get_prices_for_date.

        Nord Pool has transitioned to 15-minute MTU so the response contains
        up to 96 entries per day (4 per hour).  We group by Copenhagen local
        hour and average the slots within each hour to produce a single
        DKK/kWh value per hour.

        Prices in the response are DKK/MWh — divided by 1000 for DKK/kWh.
        Raises RuntimeError if the action fails (e.g. tomorrow not yet published).
        """
        try:
            response = await self.hass.services.async_call(
                "nordpool",
                "get_prices_for_date",
                {
                    "config_entry": self.config["nordpool_config_entry_id"],
                    "date": date_str,
                },
                blocking=True,
                return_response=True,
            )
        except Exception as err:
            raise RuntimeError(
                f"nordpool.get_prices_for_date failed for {date_str}: {err}"
            ) from err

        if not response:
            raise RuntimeError(f"Empty response from Nord Pool for {date_str}")

        # Locate area data — try exact match then case-insensitive fallback
        area_data = response.get(area)
        if area_data is None:
            for key, val in response.items():
                if key.upper() == area.upper():
                    area_data = val
                    break

        if not area_data:
            raise RuntimeError(
                f"Area '{area}' not found in Nord Pool response for {date_str}. "
                f"Available keys: {list(response.keys())}"
            )

        # Bucket all 15-min (or hourly) slots by Copenhagen local hour,
        # then average to produce one DKK/kWh value per hour.
        buckets: dict[int, list[float]] = {}
        local_tz = self._get_nordpool_tz()
        for entry in area_data:
            try:
                start = datetime.fromisoformat(entry["start"])
                local_hour = start.astimezone(local_tz).hour
                price_kwh = float(entry["price"]) / 1000.0
                buckets.setdefault(local_hour, []).append(price_kwh)
            except (KeyError, ValueError, TypeError) as err:
                _LOGGER.debug("Skipping price entry %s: %s", entry, err)
                continue

        hourly: list[float | None] = [None] * 24
        for hour, prices in buckets.items():
            if 0 <= hour < 24 and prices:
                hourly[hour] = round(sum(prices) / len(prices), 6)

        filled = sum(1 for p in hourly if p is not None)
        _LOGGER.debug(
            "Nord Pool prices for %s: %d hours filled from %d slots",
            date_str, filled, len(area_data),
        )
        return hourly

    # ------------------------------------------------------------------
    # Price adjustment
    # ------------------------------------------------------------------

    def _adjust(
        self,
        spot: float | None,
        hour: int,
        grid: list[float],
        fortjeneste: float,
        vat: float,
    ) -> float | None:
        if spot is None:
            return None
        return round((float(spot) + fortjeneste + grid[hour % 24]) * vat, 6)

    def _adjust_list(self, raw, grid, fortjeneste, vat):
        return [self._adjust(p, h, grid, fortjeneste, vat) for h, p in enumerate(raw)]

    # ------------------------------------------------------------------
    # External statistics helpers
    # ------------------------------------------------------------------

    async def _get_last_stat(
        self, stat_id: str
    ) -> tuple[float, datetime | None]:
        """
        Return (last_sum, last_utc_timestamp) for a statistic series.
        Both are used to continue cumulative sums without double-counting.
        """
        recorder = get_instance(self.hass)
        if recorder is None:
            return 0.0, None
        stats = await recorder.async_add_executor_job(
            lambda: get_last_statistics(
                self.hass, 1, stat_id, True, {"sum", "state"}
            )
        )
        if not stats or stat_id not in stats:
            return 0.0, None
        entries = stats[stat_id]
        if not entries or not isinstance(entries[0], dict):
            return 0.0, None
        entry = entries[0]
        last_sum = float(entry.get("sum") or 0.0)
        last_ts: datetime | None = None
        raw = entry.get("start")
        if raw is not None:
            try:
                if isinstance(raw, (int, float)):
                    last_ts = datetime.fromtimestamp(float(raw), tz=_UTC)
                elif isinstance(raw, datetime):
                    last_ts = raw.astimezone(_UTC)
            except (OSError, OverflowError, ValueError):
                pass
        return last_sum, last_ts

    def _make_metadata(
        self, stat_id: str, name: str, unit: str
    ) -> StatisticMetaData:
        return StatisticMetaData(
            has_mean=False,
            has_sum=True,
            mean_type=StatisticMeanType.NONE,
            name=name,
            source=DOMAIN,
            statistic_id=stat_id,
            unit_of_measurement=unit,
        )

    def _build_stat_rows(
        self,
        points: list[tuple[datetime, float]],
        initial_sum: float,
        last_ts: datetime | None,
    ) -> tuple[list[StatisticData], float, datetime | None]:
        """
        Convert (utc_datetime, value) pairs into StatisticData rows.
        Filters out any point at or before last_ts to prevent double-counting.
        Returns (rows, new_running_sum, new_last_ts).
        """
        rows: list[StatisticData] = []
        running = initial_sum
        new_last = last_ts

        for utc_start, value in sorted(points, key=lambda x: x[0]):
            if last_ts is not None and utc_start <= last_ts:
                continue
            running += value
            rows.append(StatisticData(
                start=utc_start,
                state=round(value, 4),
                sum=round(running, 4),
            ))
            new_last = utc_start

        return rows, running, new_last

    async def _insert(
        self,
        stat_id: str,
        name: str,
        unit: str,
        points: list[tuple[datetime, float]],
        initial_sum: float,
        last_ts: datetime | None,
    ) -> tuple[float, datetime | None]:
        """Insert points, returning updated (running_sum, last_ts)."""
        rows, new_sum, new_last = self._build_stat_rows(
            points, initial_sum, last_ts
        )
        if rows:
            async_add_external_statistics(
                self.hass,
                self._make_metadata(stat_id, name, unit),
                rows,
            )
            _LOGGER.debug(
                "Inserted %d rows into %s (sum now %.3f %s)",
                len(rows), stat_id, new_sum, unit,
            )
        return new_sum, new_last

    # ------------------------------------------------------------------
    # Watermark persistence
    # ------------------------------------------------------------------

    async def _load_watermarks(self) -> None:
        data = await self._store.async_load()
        self._watermarks = data or {}

    async def _save_watermarks(self) -> None:
        await self._store.async_save(self._watermarks)

    # ------------------------------------------------------------------
    # Consumption backfill
    # ------------------------------------------------------------------

    async def _backfill_consumption(
        self,
        session: aiohttp.ClientSession,
        headers: dict,
    ) -> None:
        """
        Insert all available Eloverblik consumption into HA statistics.
        On first run probes backwards until the API returns no data, then
        fetches the full range forward.  Subsequent runs are incremental.
        """
        now_dk = datetime.now(_DK_TZ)
        today_str = now_dk.strftime("%Y-%m-%d")
        wm = self._watermarks.get("consumption")

        if wm:
            # Incremental — only fetch new days
            await self._fetch_and_store_consumption(
                session, headers, wm, today_str
            )
            return

        # ── Full backfill: find oldest date with data ──────────────────
        _LOGGER.info(
            "No consumption watermark — starting full historical backfill "
            "(max %d days)", _MAX_BACKFILL_DAYS
        )
        probe_to = now_dk
        oldest_with_data: str | None = None
        probed = 0

        while probed < _MAX_BACKFILL_DAYS:
            probe_from = probe_to - timedelta(days=_FETCH_CHUNK_DAYS)
            f = probe_from.strftime("%Y-%m-%d")
            t = probe_to.strftime("%Y-%m-%d")
            try:
                pts = await self._fetch_consumption_chunk(session, headers, f, t)
            except Exception:
                break
            if not pts:
                _LOGGER.debug("No data back to %s — oldest boundary found", f)
                break
            oldest_with_data = f
            probe_to = probe_from
            probed += _FETCH_CHUNK_DAYS

        if not oldest_with_data:
            _LOGGER.warning(
                "No historical consumption data found — meter may be new."
            )
            return

        _LOGGER.info(
            "Oldest data: %s — fetching forward to %s", oldest_with_data, today_str
        )
        await self._fetch_and_store_consumption(
            session, headers, oldest_with_data, today_str
        )

    async def _fetch_and_store_consumption(
        self,
        session: aiohttp.ClientSession,
        headers: dict,
        from_date_str: str,
        to_date_str: str,
    ) -> None:
        """
        Fetch Eloverblik consumption in non-overlapping 30-day chunks from
        from_date_str to to_date_str and insert into external statistics.
        """
        mp_id = self.config["metering_point_id"]
        stat_name = f"Eloverblik consumption (…{mp_id[-4:]})"

        from_dt = datetime.strptime(from_date_str, "%Y-%m-%d").replace(tzinfo=_DK_TZ)
        to_dt   = datetime.strptime(to_date_str,   "%Y-%m-%d").replace(tzinfo=_DK_TZ)

        running_sum, last_ts = await self._get_last_stat(self.stat_id_consumption)
        cursor = from_dt

        while cursor < to_dt:
            chunk_end  = min(cursor + timedelta(days=_FETCH_CHUNK_DAYS), to_dt)
            chunk_from = cursor.strftime("%Y-%m-%d")
            # Keep to one day before chunk_end so ranges never overlap
            chunk_to   = (chunk_end - timedelta(days=1)).strftime("%Y-%m-%d")
            cursor = chunk_end

            if chunk_from > chunk_to:
                chunk_to = chunk_from

            try:
                pts = await self._fetch_consumption_chunk(
                    session, headers, chunk_from, chunk_to
                )
            except Exception as err:
                _LOGGER.warning(
                    "Consumption chunk %s→%s failed: %s — will retry",
                    chunk_from, chunk_to, err,
                )
                continue

            if not pts:
                continue
            
            self._update_recent_hourly(pts)
            
            running_sum, last_ts = await self._insert(
                self.stat_id_consumption, stat_name, "kWh",
                pts, running_sum, last_ts,
            )
            if last_ts:
                self._watermarks["consumption"] = (
                    last_ts.astimezone(_DK_TZ).strftime("%Y-%m-%d")
                )
                await self._save_watermarks()

    # ------------------------------------------------------------------
    # Cost backfill
    # ------------------------------------------------------------------

    async def _backfill_costs(
        self,
        charges_data: dict,
        area: str,
        session: aiohttp.ClientSession,
        headers: dict,
    ) -> None:
        """
        Backfill hourly cost statistics (DKK) for up to _NORDPOOL_HISTORY_DAYS
        back, using Nord Pool historical prices + Eloverblik consumption +
        date-specific grid tariffs.

        Formula per hour:
            cost_DKK = consumption_kWh × (spot_DKK_kWh + fortjeneste + tariff[h]) × VAT

        Consumption is fetched in one bulk API call for the entire date range,
        then indexed by (date, hour) so we only hit Eloverblik once.
        Nord Pool prices are fetched per day (API only accepts one date at a time).
        """
        now_dk   = datetime.now(_DK_TZ)
        today    = now_dk.date()
        mp_id    = self.config["metering_point_id"]
        stat_name = f"Eloverblik total cost (…{mp_id[-4:]})"

        fortjeneste = float(self.config.get("fortjeneste") or 0.0)
        vat = 1.0 + float(self.config.get("vat_percent", 25.0)) / 100.0

        cost_wm = self._watermarks.get("cost")
        if cost_wm:
            start_date = (
                datetime.strptime(cost_wm, "%Y-%m-%d").date()
                + timedelta(days=1)
            )
        else:
            start_date = today - timedelta(days=_NORDPOOL_HISTORY_DAYS)

        if start_date > today:
            _LOGGER.debug("Cost watermark is current — nothing to backfill")
            return

        from_str = start_date.strftime("%Y-%m-%d")
        to_str   = today.strftime("%Y-%m-%d")

        _LOGGER.warning(
            "Eloverblik: backfilling cost statistics from %s to %s",
            from_str, to_str,
        )

        # ── Step 1: Fetch ALL consumption for the range in one API call ──
        # Use (to_date + 1) so the last day is included by the Eloverblik API.
        to_plus1 = (today + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            all_cons_pts = await self._fetch_consumption_chunk(
                session, headers, from_str, to_plus1
            )
        except Exception as err:
            _LOGGER.warning(
                "Eloverblik: consumption fetch for cost backfill failed: %s", err
            )
            return

        if not all_cons_pts:
            _LOGGER.warning(
                "Eloverblik: no consumption data returned for %s → %s — "
                "cost backfill skipped", from_str, to_str,
            )
            return

        # Index by (date_str, local_hour) → kWh
        cons_index: dict[tuple[str, int], float] = {}
        for utc_ts, kwh in all_cons_pts:
            local_dt = utc_ts.astimezone(_DK_TZ)
            key = (local_dt.strftime("%Y-%m-%d"), local_dt.hour)
            cons_index[key] = kwh

        _LOGGER.warning(
            "Eloverblik: fetched %d consumption hours across %d days for cost backfill",
            len(cons_index),
            len({k[0] for k in cons_index}),
        )

        # ── Step 2: Iterate day by day, fetch NP prices, compute cost ────
        running_sum, last_ts = await self._get_last_stat(self.stat_id_cost)
        days_stored = 0
        cur = start_date

        while cur <= today:
            date_str = cur.strftime("%Y-%m-%d")

            try:
                spot_prices = await self._fetch_nordpool_prices(date_str, area)
            except RuntimeError as err:
                _LOGGER.debug("NP prices unavailable for %s: %s", date_str, err)
                cur += timedelta(days=1)
                continue

            if not any(p is not None for p in spot_prices):
                _LOGGER.debug("No NP prices for %s", date_str)
                cur += timedelta(days=1)
                continue

            grid = self._parse_charges_for_date(charges_data, cur)

            cost_points: list[tuple[datetime, float]] = []
            for hour in range(24):
                spot = spot_prices[hour]
                kwh  = cons_index.get((date_str, hour))
                if spot is None or kwh is None:
                    continue
                adj_price = (spot + fortjeneste + grid[hour]) * vat
                cost_dkk  = round(kwh * adj_price, 6)
                utc_start = (
                    datetime.strptime(date_str, "%Y-%m-%d")
                    .replace(tzinfo=_DK_TZ)
                    + timedelta(hours=hour)
                ).astimezone(_UTC)
                cost_points.append((utc_start, cost_dkk))

            if cost_points:
                running_sum, last_ts = await self._insert(
                    self.stat_id_cost, stat_name, "DKK",
                    cost_points, running_sum, last_ts,
                )
                self._watermarks["cost"] = date_str
                await self._save_watermarks()
                days_stored += 1

            cur += timedelta(days=1)

        _LOGGER.warning(
            "Eloverblik: cost backfill complete — %d days stored, "
            "running total %.2f DKK",
            days_stored, running_sum,
        )

    # ------------------------------------------------------------------
    # Main update
    # ------------------------------------------------------------------

    async def _async_update_data(self) -> dict:
        try:
            now_dk = datetime.now(_DK_TZ)
            today_str    = now_dk.strftime("%Y-%m-%d")
            tomorrow_str = (now_dk + timedelta(days=1)).strftime("%Y-%m-%d")

            # Load watermarks once per session
            if not self._watermarks:
                await self._load_watermarks()

            async with aiohttp.ClientSession() as session:
                access_token = await self._get_access_token(session)
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                }
                mp_body = {
                    "meteringPoints": {
                        "meteringPoint": [self.config["metering_point_id"]]
                    }
                }

                # 1. Consumption backfill / incremental update ───────────
                await self._backfill_consumption(session, headers)
                                
                # 2. Tariffs + fees via getcharges ───────────────────────
                async with session.post(
                    f"{ELOVERBLIK_BASE}/meteringpoints/meteringpoint/getcharges",
                    headers=headers,
                    json=mp_body,
                ) as resp:
                    resp.raise_for_status()
                    charges_data = await resp.json()

            if not isinstance(charges_data, dict):
                charges_data = {}

            # 3. Grid tariffs for today's sensors ────────────────────────
            hourly_grid_tariff = self._parse_charges_today(charges_data)

            # 4. Nord Pool prices for today and tomorrow ──────────────────
            area = self._nordpool_area()
            today_raw = await self._fetch_nordpool_prices(today_str, area)
            try:
                tomorrow_raw = await self._fetch_nordpool_prices(
                    tomorrow_str, area
                )
            except RuntimeError as err:
                _LOGGER.debug("Tomorrow prices not yet available: %s", err)
                tomorrow_raw = [None] * 24

            # 5. Cost backfill (uses same session scope via new session) ──
            async with aiohttp.ClientSession() as session2:
                access_token2 = await self._get_access_token(session2)
                headers2 = {
                    "Authorization": f"Bearer {access_token2}",
                    "Content-Type": "application/json",
                }
                await self._backfill_costs(
                    charges_data, area, session2, headers2
                )

            # 6. Adjust today's prices for live sensors ───────────────────
            fortjeneste = float(self.config.get("fortjeneste") or 0.0)
            vat = 1.0 + float(self.config.get("vat_percent", 25.0)) / 100.0

            today_adjusted    = self._adjust_list(today_raw, hourly_grid_tariff, fortjeneste, vat)
            tomorrow_adjusted = self._adjust_list(tomorrow_raw, hourly_grid_tariff, fortjeneste, vat)

            current_hour          = now_dk.hour
            current_grid_tariff   = hourly_grid_tariff[current_hour]
            current_adjusted_price = self._adjust(
                today_raw[current_hour], current_hour,
                hourly_grid_tariff, fortjeneste, vat,
            )

        except UpdateFailed:
            raise
        except aiohttp.ClientResponseError as err:
            raise UpdateFailed(
                f"Eloverblik API error {err.status}: {err.message}"
            ) from err
        except aiohttp.ClientError as err:
            raise UpdateFailed(f"Network error: {err}") from err
        except Exception as err:
            raise UpdateFailed(f"Unexpected error: {err}") from err

        return {
            "today_prices":           today_adjusted,
            "tomorrow_prices":        tomorrow_adjusted,
            "hourly_grid_tariff":     hourly_grid_tariff,
            "current_grid_tariff":    current_grid_tariff,
            "current_adjusted_price": current_adjusted_price,
            "current_hour":           current_hour,
            "today_hourly_consumption": self._recent_hourly_consumption,
            "consumption_watermark":  self._watermarks.get("consumption"),
            "cost_watermark":         self._watermarks.get("cost"),
            "stat_id_consumption":    self.stat_id_consumption,
            "stat_id_cost":           self.stat_id_cost,
        }