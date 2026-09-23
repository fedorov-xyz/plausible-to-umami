# Plausible -> Umami Importer

Move your history from a Plausible CSV export into [Umami](https://umami.is) v3 on PostgreSQL. The script writes straight into Umami's `session`, `website_event` and `event_data` tables.

Plausible exports only **daily aggregates**, so the script has to **synthesize** sessions and events from them. It does this deterministically: running it again on the same export gives the same rows and the same UUIDs.

Inspired by [JeongJuhyeon/plausible-to-umami](https://github.com/JeongJuhyeon/plausible-to-umami).

## Tested with

| Component  | Version                                           |
| ---------- | ------------------------------------------------- |
| Plausible  | `plausible/community-edition:v3.2.1` (CSV export) |
| Umami      | `v3.4.0` (latest), PostgreSQL                     |
| PostgreSQL | 17                                                |
| Node.js    | 25.6, 26.8                                        |

Test exports ranged from 172 days / 5k visits to 874 days / 1.3M visits / 8.4M events. On both, the totals Umami reports after the import match the export (see [Accuracy](#accuracy)).

## What it imports

Exact totals per day:

- **Visits, bounces and visit duration**, reproduced per entry page (`imported_entry_pages_*.csv`, `imported_visitors_*.csv`)
- **Pageviews**, overall and per page (`imported_pages_*.csv`)
- **Visitors**

Spread across sessions from the daily breakdowns:

- **Devices**, **browsers** and **operating systems**, mapped to the names Umami's `detect-browser` produces (`Mac` -> `Mac OS`, `Chrome` on iOS -> `crios`, …)
- **Country, region and city** (`imported_locations_*.csv`). Plausible stores cities as GeoNames IDs. The script resolves them to names using the GeoNames `allCountries` dump
- **Referrers and UTM parameters** (`imported_sources_*.csv`), set on the entry pageview like live traffic
- **Custom events** (`imported_custom_events_*.csv`), with `url` / `path` saved as event data

## What it does not import

- Exit pages (Umami derives them from events)
- Custom properties other than `url` / `path`
- Scroll depth and time on page
- Screen size, language, browser and OS versions (not in the export)
- Plausible's internal `engagement` event

## Assumptions and limitations

- Session attributes (device, browser, OS, location, source) come from separate daily breakdowns. The script pairs them in a plausible way (iOS ↔ mobile ↔ Safari), but real correlations across dimensions are lost.
- Times of day are random within the day in the site's time zone. Hourly charts for imported days are not meaningful.
- A few GeoNames IDs from Plausible may be missing from the current dump (deleted or merged records). Those sessions get an empty city, and the script prints the IDs with their visitor counts.

## Requirements

- Node.js from [`.nvmrc`](.nvmrc) (`nvm use`)
- System `unzip`, used to read the GeoNames dump
- A running Umami v3 on PostgreSQL, and the target website **already created in Umami**
- About 420 MB of free space in the OS temp directory. The GeoNames `allCountries.zip` is downloaded there on the first run and reused later

## Usage

### 1. Export from Plausible

In Plausible: **Site Settings → Imports & Exports → Export to CSV**. Download the zip and unpack it into a folder. It should look like this:

```
plausible-export/
├── imported_browsers_20241224_20260923.csv
├── imported_custom_events_20241224_20260923.csv
├── imported_custom_props_20241224_20260923.csv
├── imported_devices_20241224_20260923.csv
├── imported_entry_pages_20241224_20260923.csv
├── imported_exit_pages_20241224_20260923.csv
├── imported_locations_20241224_20260923.csv
├── imported_operating_systems_20241224_20260923.csv
├── imported_pages_20241224_20260923.csv
├── imported_sources_20241224_20260923.csv
└── imported_visitors_20241224_20260923.csv
```

Only `imported_visitors_*` and `imported_pages_*` are required. The rest are used if present.

### 2. Install

```bash
git clone https://github.com/fedorov-xyz/plausible-to-umami.git
cd plausible-to-umami
nvm use
npm ci
cp .env.example .env
```

Put your Umami database connection string into `.env` as `UMAMI_DATABASE_URL`. `npm run migrate` loads it automatically. You can also export the variable in your shell instead.

### 3. Dry run

Without `UMAMI_DATABASE_URL`, the script generates everything in memory and prints the totals Umami should show:

```bash
npm run migrate -- ./plausible-export \
  --website-id <umami-website-uuid> \
  --tz Europe/Moscow \
  --date-to 2026-09-22 \
  --dry-run
```

With `UMAMI_DATABASE_URL` set, `--dry-run` also checks the website in the database, still without writing anything.

### 4. Import

**Back up your Umami database first.** Make sure `UMAMI_DATABASE_URL` is set in `.env`.

```bash
npm run migrate -- ./plausible-export \
  --website-id <umami-website-uuid> \
  --tz Europe/Moscow \
  --date-to 2026-09-22
```

The whole import runs in a single transaction. Before committing, the script runs Umami's own stats query against the new rows and prints a "plausible vs umami" comparison.

### Roll back

```bash
npm run migrate -- --website-id <umami-website-uuid> --rollback
```

This removes all imported rows (tagged `plausible-import`) from the website. Add `--dry-run` to only see how many events would be deleted.

## Options

| Option                     | Description                                                                                                     |
| -------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `<export-dir>`             | Unpacked Plausible export folder (first argument, not needed with `--rollback`)                                 |
| `--website-id <uuid>`      | **Required.** Target website in Umami, must already exist                                                       |
| `--tz <iana>`              | **Required for import.** The site's _Reporting timezone_ in Plausible, e.g. `Europe/Berlin`                     |
| `--date-from <YYYY-MM-DD>` | First day to import                                                                                             |
| `--date-to <YYYY-MM-DD>`   | Last day to import. Use the day before Umami tracking started. The last day in the export is usually incomplete |
| `--dry-run`                | Generate and verify, but don't write                                                                            |
| `--replace`                | Delete the previous import for this website in the same transaction                                             |
| `--allow-overlap`          | Import even if the website already has live events in the date range                                            |
| `--rollback`               | Delete the import from the website and exit                                                                     |

| Environment variable | Description                                                                                                           |
| -------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `UMAMI_DATABASE_URL` | PostgreSQL connection string of the Umami database, read from `.env` or the shell. Without it, only `--dry-run` works |

### Why `--tz` matters

The export only contains dates. To turn a day into real timestamps, the script needs to know where that day starts. With `Europe/Moscow`, `2026-04-05` becomes `2026-04-04T21:00Z … 2026-04-05T21:00Z`. DST days of 23 or 25 hours are handled.

Umami stores `created_at` as `timestamptz` and picks the display time zone in its UI, so the database's own time zone doesn't matter. A wrong `--tz` keeps the period totals correct but shifts every event by the zone difference, which distorts daily charts and the boundary with live data.

## Safety checks

Before writing, the script checks that:

- the website exists and isn't deleted
- the website domain matches the hostname in the export (warning only)
- the website's `reset_at` doesn't hide the imported range (warning only)
- there is no previous import. Otherwise it asks for `--replace`
- there are no live events in the date range. Otherwise it asks for `--allow-overlap`

## Accuracy

Visitors, visits, pageviews and average visit duration match Plausible exactly. Bounces can differ by a handful: Plausible counts a single-pageview visit as non-bounced if it only had an `engagement` event, and that event isn't imported. On the large test export that was 9 bounces out of 112k.

## How it works

For each day:

1. **Visits by entry page.** Each `imported_entry_pages` row becomes `entrances` visits: `bounces` single-pageview visits and the rest multi-pageview visits. Pageviews and duration are split among them with exponential weights.
2. **Fit to the daily pageview total.** Visits crossing midnight make entry pages and daily totals disagree, so pageviews are added to or taken from non-bounced visits until they match.
3. **Move duration.** Umami gives single-pageview visits zero time, so their duration goes to a multi-pageview visit. Visits longer than a day give the excess to other visits.
4. **Pages after the entry page** come from the day's pool of "page pageviews minus entrances", shuffled.
5. **Timestamps.** The first pageview is at a random time within the local day and the last one exactly `duration` later, so Umami's max − min equals Plausible's visit duration.
6. **Sessions.** `min(visitors, visits)` sessions per day, each with at least one visit. OS, device, browser and location slots are expanded from the daily breakdowns.
7. **Sources** go to the entry pageview. Bounce-source slots go to bounced visits.
8. **Custom events** first go to non-bounced single-pageview visits (otherwise Umami would count them as bounces), then to random non-bounced visits.

Rows are generated one day at a time and inserted in batches sized to Postgres's 65535-parameter limit, so large exports don't need much memory.

## Compatibility with newer Umami versions

The table columns and the stats query are checked against Umami `v3.4.0`. Before using a newer version, check these files in the Umami repo for changes:

- `prisma/schema.prisma`
- `src/lib/constants.ts`
- `src/lib/detect.ts`
- `src/queries/sql/getWebsiteStats.ts`

## License

[MIT](LICENSE)
