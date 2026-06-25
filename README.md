# Eloverblik Nordpool Integration

Combines Eloverblik hourly consumption data with Nord Pool spot prices to create:
- Long-term energy statistics (`eloverblik_nordpool:consumption_...` and `eloverblik_nordpool:total_cost_...`)
- Hourly sensors (today/tomorrow adjusted prices + grid tariffs)
- Direct Energy Dashboard support (consumption + cost entities)

## Installation
1. HACS → Integrations → Add repository → `GKHerping/eloverblik-nordpool`
2. Restart Home Assistant
3. Configuration → Integrations → Add "Eloverblik Nordpool Integration"

## Configuration
All via config flow:
- Eloverblik refresh token
- Metering point ID
- Nord Pool config entry ID
- Optional: fortjeneste (DKK/kWh), VAT %

## Energy Dashboard
- Grid consumption → `eloverblik_nordpool:consumption_...`
- Cost → `eloverblik_nordpool:total_cost_...` (auto-linked)

## Support
- [Issues](https://github.com/YOUR_GITHUB_USERNAME/eloverblik-nordpool/issues)
- [Documentation](https://github.com/YOUR_GITHUB_USERNAME/eloverblik-nordpool/blob/main/README.md)
