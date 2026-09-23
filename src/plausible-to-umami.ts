/**
 * Migrates a Plausible CSV export (one site, unpacked folder) into Umami v3 on PostgreSQL,
 * writing directly into the `session`, `website_event` and `event_data` tables.
 *
 * Plausible exports only daily aggregates, so events are synthesized. Visits, bounces, visit
 * duration, pageviews and entry pages are reproduced exactly (per entry page, and therefore per
 * day); visitors and per-page pageviews per day. Devices, browsers, OS, geo, sources and custom
 * events are spread across sessions from the daily breakdowns. Generation is deterministic
 *
 * All events are tagged `plausible-import`: `--replace` and `--rollback` rely on it.
 * City names come from GeoNames allCountries (downloaded once into the OS temp directory)
 *
 * Schema checked against umami-software/umami v3.4.0 (`prisma/schema.prisma`)
 *
 * Usage:
 *   npx tsx --env-file=.env src/plausible-to-umami.ts ./plausible-export --website-id <uuid> --tz Europe/Moscow --dry-run
 *
 *   --website-id <uuid>       Umami website, must already exist
 *   --tz <iana>               the site's timezone in Plausible
 *   --date-from <YYYY-MM-DD>  first day to import
 *   --date-to <YYYY-MM-DD>    last day to import, the day before Umami tracking started
 *   --dry-run                 build and verify everything, don't write to the database
 *   --replace                 delete the previous import in the same transaction
 *   --allow-overlap           import even if the range already has live events
 *   --rollback                delete the import from the website and exit (no export-dir needed)
 *
 * UMAMI_DATABASE_URL — connection string of the Umami database
 */

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  createWriteStream,
  existsSync,
  mkdirSync,
  readdirSync,
  renameSync,
  statSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import type { ReadableStream } from 'node:stream/web';
import { parseArgs } from 'node:util';

import { parseFile } from '@fast-csv/parse';
import { groupBy, sumBy, uniq } from 'es-toolkit';
import pg from 'pg';
import { uniformFloat64 } from 'pure-rand/distribution/uniformFloat64';
import { uniformInt } from 'pure-rand/distribution/uniformInt';
import { xoroshiro128plusFromState } from 'pure-rand/generator/xoroshiro128plus';
import type { RandomGenerator } from 'pure-rand/types/RandomGenerator';
import { v7 as uuidv7 } from 'uuid';

const TAG = 'plausible-import';

// `engagement` is Plausible's internal time-on-page event, Umami has no equivalent
const SKIP_EVENTS = new Set(['engagement']);

// Umami src/lib/constants.ts: EVENT_TYPE, DATA_TYPE
const EVENT_TYPE_PAGEVIEW = 1;
const EVENT_TYPE_CUSTOM = 2;
const DATA_TYPE_STRING = 1;

const GEONAMES_DIR = join(tmpdir(), 'plausible-to-umami');
const GEONAMES_ZIP = join(GEONAMES_DIR, 'allCountries.zip');
const GEONAMES_URL = 'https://download.geonames.org/export/dump/allCountries.zip';

const { values: opt, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    'website-id': { type: 'string' },
    tz: { type: 'string' },
    'date-from': { type: 'string' },
    'date-to': { type: 'string' },
    'dry-run': { type: 'boolean', default: false },
    replace: { type: 'boolean', default: false },
    'allow-overlap': { type: 'boolean', default: false },
    rollback: { type: 'boolean', default: false },
  },
});

function die(msg: string): never {
  console.error(`\nERROR: ${msg}\n`);
  process.exit(1);
}

const websiteId = opt['website-id'] ?? die('--website-id is required');
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(websiteId)) {
  die('--website-id must be a UUID');
}

const databaseUrl = process.env.UMAMI_DATABASE_URL;
const dryRun = opt['dry-run'];

// ----------------------------------------------------------------------------- deterministic rng

// xoroshiro128+ seeded with 128 bits of sha256(seed): rerunning on the same data yields the same
// ids, so --replace doesn't reshuffle anything
class Rng {
  private gen: RandomGenerator;

  constructor(seed: string) {
    const h = createHash('sha256').update(seed).digest();
    this.gen = xoroshiro128plusFromState([0, 4, 8, 12].map((o) => h.readInt32BE(o)));
  }

  // [0, 1)
  next(): number {
    return uniformFloat64(this.gen);
  }

  int(min: number, max: number): number {
    return uniformInt(this.gen, min, max);
  }

  expo(): number {
    return -Math.log(1 - this.next());
  }

  pick<T>(arr: T[]): T {
    return arr[this.int(0, arr.length - 1)];
  }

  shuffle<T>(arr: T[]): T[] {
    for (let i = arr.length - 1; i > 0; i--) {
      const j = this.int(0, i);
      [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
  }

  sample<T>(arr: T[], k: number): T[] {
    return this.shuffle([...arr]).slice(0, k);
  }

  // UUIDv7 like Umami with USE_UUIDV7: ids of rows inserted in time order land at the end of the
  // indexes instead of random pages
  uuid(time: Date | number): string {
    const random = Uint8Array.from({ length: 16 }, () => this.int(0, 255));
    return uuidv7({ msecs: typeof time === 'number' ? time : time.getTime(), random });
  }
}

// ----------------------------------------------------------------------------- export reading

type Row = Record<string, string>;

const PREFIXES = [
  'visitors',
  'pages',
  'entry_pages',
  'devices',
  'browsers',
  'operating_systems',
  'locations',
  'sources',
  'custom_events',
] as const;
type Prefix = (typeof PREFIXES)[number];

async function readCsv(path: string): Promise<Row[]> {
  const rows: Row[] = [];
  for await (const row of parseFile<Row, Row>(path, { headers: true, ignoreEmpty: true })) {
    rows.push(row);
  }
  return rows;
}

async function readExport(dir: string): Promise<Record<Prefix, Row[]>> {
  if (!existsSync(dir) || !statSync(dir).isDirectory()) die(`export folder not found: ${dir}`);
  const files = readdirSync(dir).filter((f) => f.endsWith('.csv'));

  const result = {} as Record<Prefix, Row[]>;
  for (const p of PREFIXES) {
    // exact prefix: `imported_pages_` must not match `imported_entry_pages_`
    const name = files
      .filter((f) => f.startsWith(`imported_${p}_`))
      .sort()
      .pop();
    result[p] = name ? await readCsv(join(dir, name)) : [];
  }
  if (!result.visitors.length || !result.pages.length) {
    die('imported_visitors_*.csv and imported_pages_*.csv are required');
  }
  return result;
}

function byDay(rows: Row[]): Map<string, Row[]> {
  return new Map(Object.entries(groupBy(rows, (r) => r.date)));
}

const toInt = (v: string | undefined) => {
  const n = Math.trunc(Number((v ?? '').trim() || 0));
  return Number.isFinite(n) ? n : 0;
};

const trunc = (v: string | null | undefined, n: number) => {
  const s = (v ?? '').trim();
  return s ? s.slice(0, n) : null;
};

// ----------------------------------------------------------------------------- geonames

async function downloadGeonames() {
  mkdirSync(GEONAMES_DIR, { recursive: true });
  console.log(`Downloading ${GEONAMES_URL} -> ${GEONAMES_ZIP} ...`);
  const res = await fetch(GEONAMES_URL);
  if (!res.ok || !res.body) die(`GeoNames download failed: ${res.status} ${res.statusText}`);
  // .part, so an interrupted download isn't mistaken for the cache on the next run
  const part = `${GEONAMES_ZIP}.part`;
  await pipeline(Readable.fromWeb(res.body as ReadableStream), createWriteStream(part));
  renameSync(part, GEONAMES_ZIP);
}

/** City names by GeoNames ID (dump format: `id \t name \t asciiname \t …`) */
async function resolveGeonames(ids: Set<string>): Promise<Map<string, string>> {
  const found = new Map<string, string>();
  if (!ids.size) return found;
  if (existsSync(GEONAMES_ZIP)) console.log(`GeoNames: using cached ${GEONAMES_ZIP}`);
  else await downloadGeonames();

  // unpacked allCountries.txt is ~1.6 GB, so stream it from the archive without saving
  const unzip = spawn('unzip', ['-p', GEONAMES_ZIP, 'allCountries.txt'], {
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  const exited = new Promise<number | null>((resolve, reject) => {
    unzip.on('error', reject);
    unzip.on('close', resolve);
  });
  for await (const line of createInterface({ input: unzip.stdout, crlfDelay: Infinity })) {
    const tab = line.indexOf('\t');
    const id = line.slice(0, tab);
    if (!ids.has(id)) continue;
    found.set(id, line.slice(tab + 1, line.indexOf('\t', tab + 1)));
    if (found.size === ids.size) {
      unzip.kill();
      break;
    }
  }
  const code = await exited;
  if (found.size < ids.size && code !== 0) die(`unzip exited with code ${code}`);
  return found;
}

// ----------------------------------------------------------------------------- Umami value maps

// Umami fills os/browser from detect-browser, so map Plausible names to its vocabulary
function mapOs(v: string): string | null {
  const s = v.trim().toLowerCase();
  if (!s) return null;
  const table: [string, string][] = [
    ['ipados', 'iOS'],
    ['ios', 'iOS'],
    ['android', 'Android OS'],
    ['harmony', 'Android OS'],
    ['mac', 'Mac OS'],
    ['windows', 'Windows 10'],
    ['chrome os', 'Chrome OS'],
    ['linux', 'Linux'],
    ['ubuntu', 'Linux'],
    ['fedora', 'Linux'],
    ['freebsd', 'FreeBSD'],
  ];
  for (const [needle, out] of table) if (s.includes(needle)) return out;
  return v.trim().slice(0, 20);
}

function mapDevice(v: string): string | null {
  const s = v.trim().toLowerCase();
  const known: Record<string, string> = { desktop: 'desktop', mobile: 'mobile', tablet: 'tablet' };
  return known[s] ?? (s.slice(0, 20) || null);
}

function mapBrowser(v: string, os: string | null): string | null {
  const s = v.trim().toLowerCase();
  if (!s) return null;
  const ios = os === 'iOS';
  if (s.includes('yandex')) return 'yandexbrowser';
  if (s.includes('samsung')) return 'samsung';
  if (s.includes('miui')) return 'miui';
  if (s.includes('edge')) return ios ? 'edge-ios' : 'edge-chromium';
  if (s.includes('opera mini')) return 'opera-mini';
  if (s.includes('opera')) return 'opera';
  if (s.includes('firefox')) return ios ? 'fxios' : 'firefox';
  if (s === 'mobile app' || s.includes('webview') || s.includes('google search app')) {
    return ios ? 'ios-webview' : 'chromium-webview';
  }
  if (s.includes('chrome') || s.includes('chromium') || s.includes('brave'))
    return ios ? 'crios' : 'chrome';
  if (s.includes('safari')) return ios ? 'ios' : 'safari';
  if (s.includes('huawei') || s === 'android browser') return 'android';
  if (s.includes('silk')) return 'silk';
  if (s.includes('facebook')) return 'facebook';
  if (s.includes('instagram')) return 'instagram';
  return s.slice(0, 20);
}

// OS, device and browser breakdowns are independent; sorting by these keys before joining
// pairs up plausible combinations (iOS ↔ mobile ↔ Safari) instead of random ones
const OS_ORDER: Record<string, number> = {
  iOS: 0,
  'Android OS': 1,
  'Mac OS': 3,
  'Windows 10': 4,
  Linux: 5,
  'Chrome OS': 6,
};
const DEV_ORDER: Record<string, number> = { mobile: 0, tablet: 1, desktop: 3 };
const BROWSER_ORDER: Record<string, number> = {
  safari: 0,
  'mobile app': 0,
  'samsung browser': 1,
  'miui browser': 1,
  'huawei browser mobile': 1,
  'yandex browser': 2,
  chrome: 2,
};

type Referrer = [domain: string | null, path: string | null, query: string | null];

function parseReferrer(ref: string): Referrer {
  ref = ref.trim();
  if (!ref) return [null, null, null];
  try {
    const u = new URL(ref.includes('://') ? ref : `https://${ref}`);
    const domain = (u.hostname || ref).replace(/^www\./, '');
    return [domain.slice(0, 500), (u.pathname || '/').slice(0, 500), u.search.slice(1) || null];
  } catch {
    return [ref.replace(/^www\./, '').slice(0, 500), '/', null];
  }
}

// ----------------------------------------------------------------------------- timezone math

const dtfCache = new Map<string, Intl.DateTimeFormat>();

function tzOffsetMs(instant: number, zone: string): number {
  let f = dtfCache.get(zone);
  if (!f) {
    f = new Intl.DateTimeFormat('en-US', {
      timeZone: zone,
      hourCycle: 'h23',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    dtfCache.set(zone, f);
  }
  const p = Object.fromEntries(f.formatToParts(new Date(instant)).map((x) => [x.type, x.value]));
  return Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second) - instant;
}

/** UTC instant of local midnight of `day` (YYYY-MM-DD) in `zone` */
function localMidnight(day: string, zone: string): number {
  const [y, m, d] = day.split('-').map(Number);
  const guess = Date.UTC(y, m - 1, d);
  // the second pass fixes the offset on DST transition days
  const t = guess - tzOffsetMs(guess, zone);
  return guess - tzOffsetMs(t, zone);
}

const nextDay = (day: string) =>
  new Date(Date.parse(`${day}T00:00:00Z`) + 86400000).toISOString().slice(0, 10);

// ----------------------------------------------------------------------------- generation

type Session = {
  id: string;
  createdAt: Date | null;
  browser: string | null;
  os: string | null;
  device: string | null;
  country: string | null;
  region: string | null;
  city: string | null;
};

type Visit = {
  id: string;
  entry: string;
  bounced: boolean;
  pageviews: number;
  duration: number;
  session?: Session;
  start: number;
  pages: string[];
  times: number[];
  ref: Referrer;
  utm: Record<string, string>;
};

type DbRow = (string | number | Date | null)[];

// splits total into k integer parts with exponential weights, each at least minimum
function splitInt(total: number, k: number, rng: Rng, minimum = 0): number[] {
  if (k <= 0) return [];
  let base: number[] = Array(k).fill(minimum);
  let rest = total - minimum * k;
  if (rest < 0) {
    base = Array(k).fill(0);
    rest = total;
  }
  const w = Array.from({ length: k }, () => rng.expo());
  const sw = w.reduce((a, b) => a + b, 0) || 1;
  const parts = w.map((x) => Math.floor((rest * x) / sw));
  let rem = rest - parts.reduce((a, b) => a + b, 0);
  for (const i of rng.shuffle([...Array(k).keys()])) {
    if (rem-- <= 0) break;
    parts[i]++;
  }
  return base.map((b, i) => b + parts[i]);
}

// expands a "value × visitors" breakdown into n slots; missing ones are null
function expand(
  rows: Row[],
  keys: string[],
  countField: string,
  n: number,
  rng: Rng,
): (string[] | null)[] {
  const pool: (string[] | null)[] = [];
  for (const r of rows) {
    const key = keys.map((k) => (r[k] ?? '').trim());
    for (let i = toInt(r[countField]); i > 0; i--) pool.push(key);
  }
  rng.shuffle(pool);
  const out = pool.slice(0, n);
  while (out.length < n) out.push(null);
  return out;
}

const UTM_KEYS = ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'];

// custom_events columns -> property names Plausible shows them under in custom_props
const CUSTOM_EVENT_PROPS: [column: string, key: string][] = [
  ['link_url', 'url'],
  ['path', 'path'],
];

type Stats = {
  pageviews: number;
  visitors: number;
  visits: number;
  bounces: number;
  totaltime: number;
};

type Notes = { customEvents: number; nonbounce1pv: number; poolPadded: number };

type DayRows = { sessions: DbRow[]; events: DbRow[]; eventData: DbRow[]; umami: Stats };

function selectDays(data: Record<Prefix, Row[]>): string[] {
  let days = uniq(data.visitors.map((r) => r.date)).sort();
  const dateFrom = opt['date-from'];
  const dateTo = opt['date-to'];
  if (dateFrom) days = days.filter((d) => d >= dateFrom);
  if (dateTo) days = days.filter((d) => d <= dateTo);
  if (!days.length) die('no days to import after date filters');
  return days;
}

// one day at a time: on large exports (millions of pageviews) all rows don't fit in memory at once
function* generateDays(
  data: Record<Prefix, Row[]>,
  days: string[],
  cityNames: Map<string, string>,
  rng: Rng,
  tz: string,
  notes: Notes,
): Generator<DayRows> {
  const visitors = byDay(data.visitors);
  const pages = byDay(data.pages);
  const entries = byDay(data.entry_pages);
  const devices = byDay(data.devices);
  const browsers = byDay(data.browsers);
  const oses = byDay(data.operating_systems);
  const locs = byDay(data.locations);
  const sources = byDay(data.sources);
  const cevents = byDay(data.custom_events);
  const hostname = exportHostname(data);

  for (const day of days) {
    const tot = visitors.get(day)![0];
    const nVisitors = toInt(tot.visitors);
    const nVisits = toInt(tot.visits);
    if (nVisits === 0) continue;
    const dayStart = localMidnight(day, tz);
    // 23 or 25 hours on DST transition days
    const daySec = Math.round((localMidnight(nextDay(day), tz) - dayStart) / 1000) - 1;

    // visits by entry page: bounces, duration and pageviews match Plausible exactly
    const visits: Visit[] = [];
    const erows = entries.get(day) ?? [
      {
        entry_page: '/',
        entrances: tot.visits,
        bounces: tot.bounces,
        pageviews: tot.pageviews,
        visit_duration: tot.visit_duration,
      },
    ];
    for (const r of erows) {
      const n = toInt(r.entrances);
      if (!n) continue;
      const b = Math.min(toInt(r.bounces), n);
      const nb = n - b;
      const pvSplit = splitInt(Math.max(toInt(r.pageviews) - b, nb), nb, rng, 2);
      const durSplit = splitInt(toInt(r.visit_duration), nb, rng);
      const mk = (bounced: boolean, pv: number, dur: number): Visit => ({
        id: '',
        entry: r.entry_page,
        bounced,
        pageviews: pv,
        duration: dur,
        start: 0,
        pages: [],
        times: [],
        ref: [null, null, null],
        utm: {},
      });
      for (let i = 0; i < b; i++) visits.push(mk(true, 1, 0));
      for (let i = 0; i < nb; i++) {
        if (pvSplit[i] < 2) notes.nonbounce1pv++;
        visits.push(mk(false, Math.max(pvSplit[i], 1), durSplit[i]));
      }
    }

    // fit to the daily total: visits crossing midnight make entry rows disagree with the day
    let diff = sumBy(visits, (v) => v.pageviews) - toInt(tot.pageviews);
    while (diff > 0) {
      // cutting down to one pageview comes last: Umami counts such a visit as a bounce
      // unless it gets a custom event
      let cand = visits.filter((v) => v.pageviews > 2);
      if (!cand.length) cand = visits.filter((v) => !v.bounced && v.pageviews > 1);
      if (!cand.length) break;
      if (cand[0].pageviews === 2) notes.nonbounce1pv++;
      rng.pick(cand).pageviews--;
      diff--;
    }
    const nbAll = visits.filter((v) => !v.bounced);
    while (diff < 0 && nbAll.length) {
      rng.pick(nbAll).pageviews++;
      diff++;
    }

    // a single-pageview visit has zero time in Umami (custom events don't count),
    // so its duration goes to a multi-pageview visit with the same entry page
    const multi = visits.filter((v) => v.pageviews > 1);
    const multiByEntry = groupBy(multi, (v) => v.entry);
    for (const v of visits) {
      if (v.pageviews > 1 || !v.duration) continue;
      const receivers = multiByEntry[v.entry] ?? multi;
      if (!receivers.length) continue;
      rng.pick(receivers).duration += v.duration;
      v.duration = 0;
    }
    // a visit must fit within a day: the excess goes to other visits of the day with room left
    let overflow = 0;
    for (const v of multi) {
      if (v.duration > daySec) {
        overflow += v.duration - daySec;
        v.duration = daySec;
      }
    }
    for (const v of overflow ? rng.shuffle([...multi]) : []) {
      const add = Math.min(overflow, daySec - v.duration);
      v.duration += add;
      overflow -= add;
      if (!overflow) break;
    }

    // non-entry pageviews: the day's page pool minus entrances
    const poolCnt = new Map<string, number>();
    for (const r of pages.get(day) ?? [])
      poolCnt.set(r.page, (poolCnt.get(r.page) ?? 0) + toInt(r.pageviews));
    for (const v of visits) poolCnt.set(v.entry, (poolCnt.get(v.entry) ?? 0) - 1);
    let pool: string[] = [];
    for (const [p, c] of poolCnt) for (let i = 0; i < c; i++) pool.push(p);
    rng.shuffle(pool);
    const need = sumBy(visits, (v) => v.pageviews - 1);
    if (pool.length < need) {
      const filler = [...poolCnt].sort((a, b) => b[1] - a[1])[0]?.[0] ?? '/';
      notes.poolPadded += need - pool.length;
      while (pool.length < need) pool.push(filler);
    }
    pool = pool.slice(0, need);
    let pi = 0;
    for (const v of visits)
      v.pages = [v.entry, ...Array.from({ length: v.pageviews - 1 }, () => pool[pi++])];

    // first pageview at start, last at start + duration: Umami computes visit time as
    // max - min over pageviews, so it matches Plausible's visit_duration
    for (const v of visits) {
      const d = Math.min(v.duration, daySec);
      v.start = dayStart + rng.int(0, daySec - d) * 1000;
      v.id = rng.uuid(v.start);
      if (v.pageviews === 1) {
        v.times = [v.start];
      } else {
        const mids = Array.from({ length: v.pageviews - 2 }, () => rng.int(0, d)).sort(
          (a, b) => a - b,
        );
        v.times = [0, ...mids, d].map((s) => v.start + s * 1000);
      }
    }

    // sessions = visitors of the day: OS/device/browser are correlated, geo is independent
    const nSess = Math.max(1, Math.min(nVisitors, nVisits));
    const daySessions: Session[] = Array.from({ length: nSess }, () => ({
      id: '',
      createdAt: null,
      browser: null,
      os: null,
      device: null,
      country: null,
      region: null,
      city: null,
    }));
    const osOrder = (x: string[] | null) => OS_ORDER[x ? (mapOs(x[0]) ?? '') : ''] ?? 9;
    const devOrder = (x: string[] | null) => DEV_ORDER[x ? (mapDevice(x[0]) ?? '') : ''] ?? 9;
    const brOrder = (x: string[] | null) => BROWSER_ORDER[x?.[0].toLowerCase() ?? ''] ?? 5;
    const osV = expand(oses.get(day) ?? [], ['operating_system'], 'visitors', nSess, rng).sort(
      (a, b) => osOrder(a) - osOrder(b),
    );
    const devV = expand(devices.get(day) ?? [], ['device'], 'visitors', nSess, rng).sort(
      (a, b) => devOrder(a) - devOrder(b),
    );
    const brV = expand(browsers.get(day) ?? [], ['browser'], 'visitors', nSess, rng).sort(
      (a, b) => brOrder(a) - brOrder(b),
    );
    const locV = expand(locs.get(day) ?? [], ['country', 'region', 'city'], 'visitors', nSess, rng);
    daySessions.forEach((s, i) => {
      s.os = osV[i] ? mapOs(osV[i][0]) : null;
      s.device = devV[i] ? mapDevice(devV[i][0]) : null;
      s.browser = brV[i] ? mapBrowser(brV[i][0], s.os) : null;
      const l = locV[i];
      if (l) {
        s.country = l[0].toUpperCase().slice(0, 2) || null;
        // region in the export is already in Umami format: `RU-MOW`
        s.region = l[1] && l[1] !== '0' ? l[1].slice(0, 20) : null;
        s.city = l[2] && l[2] !== '0' ? trunc(cityNames.get(l[2]), 50) : null;
      }
    });
    rng.shuffle(daySessions);

    // visits -> sessions, each session gets at least one visit
    rng.shuffle(visits);
    visits.forEach((v, i) => {
      v.session = i < nSess ? daySessions[i] : rng.pick(daySessions);
    });
    for (const v of visits) {
      const s = v.session!;
      const t = new Date(v.start);
      if (!s.createdAt || t < s.createdAt) s.createdAt = t;
    }
    for (const s of daySessions) {
      s.createdAt ??= new Date(dayStart);
      s.id = rng.uuid(s.createdAt);
    }

    // sources: bounce slots go to bounced visits, the rest to the others
    type Src = [referrer: string, utm: Record<string, string>];
    const srcB: Src[] = [];
    const srcNb: Src[] = [];
    for (const r of sources.get(day) ?? []) {
      const utm = Object.fromEntries(UTM_KEYS.filter((k) => r[k]).map((k) => [k, r[k]]));
      const item: Src = [r.referrer, utm];
      const vb = toInt(r.bounces);
      for (let i = 0; i < vb; i++) srcB.push(item);
      for (let i = Math.max(toInt(r.visits) - vb, 0); i > 0; i--) srcNb.push(item);
    }
    rng.shuffle(srcB);
    rng.shuffle(srcNb);
    for (const v of visits) {
      let lst = v.bounced ? srcB : srcNb;
      if (!lst.length) lst = srcB.length ? srcB : srcNb;
      const it = lst.pop();
      if (it) {
        v.ref = parseReferrer(it[0]);
        v.utm = it[1];
      }
    }

    // custom events go first to non-bounced single-pageview visits: in Plausible they are
    // not bounces precisely because of the event, and without one Umami would count them as bounces
    const nonb = visits.filter((v) => !v.bounced);
    const targetsPool = nonb.length ? nonb : visits;
    let single = targetsPool.filter((v) => v.pageviews === 1);
    const custom: [Visit, string, Record<string, string>][] = [];
    for (const r of cevents.get(day) ?? []) {
      if (SKIP_EVENTS.has(r.name.toLowerCase())) continue;
      const k0 = Math.max(1, Math.min(toInt(r.visitors), targetsPool.length));
      const first = single.slice(0, k0);
      single = single.slice(k0);
      const firstSet = new Set(first);
      const rest = targetsPool.filter((v) => !firstSet.has(v));
      const targets = [...first, ...rng.sample(rest, k0 - first.length)];
      const props: Record<string, string> = {};
      for (const [column, key] of CUSTOM_EVENT_PROPS) if (r[column]) props[key] = r[column];
      splitInt(toInt(r.events), targets.length, rng, 1).forEach((cnt, i) => {
        for (let c = 0; c < cnt; c++) custom.push([targets[i], r.name.slice(0, 50), props]);
      });
    }

    const events: DbRow[] = [];
    const eventData: DbRow[] = [];
    for (const v of visits) {
      v.pages.forEach((page, i) => {
        const qi = page.indexOf('?');
        const path = qi === -1 ? page : page.slice(0, qi);
        const q = qi === -1 ? '' : page.slice(qi + 1);
        // utm and referrer only on the entry pageview, like live traffic
        const utm = i === 0 ? v.utm : {};
        const query = q || (Object.keys(utm).length ? new URLSearchParams(utm).toString() : null);
        const [rd, rp, rq] = i === 0 ? v.ref : [null, null, null];
        events.push([
          rng.uuid(v.times[i]),
          websiteId,
          v.session!.id,
          v.id,
          new Date(v.times[i]),
          path.slice(0, 500) || '/',
          trunc(query, 500),
          trunc(utm.utm_source, 255),
          trunc(utm.utm_medium, 255),
          trunc(utm.utm_campaign, 255),
          trunc(utm.utm_content, 255),
          trunc(utm.utm_term, 255),
          rp,
          trunc(rq, 500),
          rd,
          EVENT_TYPE_PAGEVIEW,
          null,
          TAG,
          hostname,
        ]);
      });
    }
    for (const [v, name, props] of custom) {
      const ts = new Date(v.times[0] + (v.times[v.times.length - 1] - v.times[0]) * rng.next());
      const eid = rng.uuid(ts);
      const path = rng.pick(v.pages).split('?')[0];
      events.push([
        eid,
        websiteId,
        v.session!.id,
        v.id,
        ts,
        path,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        EVENT_TYPE_CUSTOM,
        name,
        TAG,
        hostname,
      ]);
      for (const [key, val] of Object.entries(props)) {
        eventData.push([
          rng.uuid(ts),
          websiteId,
          eid,
          key.slice(0, 500),
          val.slice(0, 500),
          DATA_TYPE_STRING,
          ts,
        ]);
      }
      notes.customEvents++;
    }
    const sessions: DbRow[] = daySessions.map((s) => [
      s.id,
      websiteId,
      s.browser,
      s.os,
      s.device,
      null,
      null,
      s.country,
      s.region,
      s.city,
      null,
      s.createdAt,
    ]);
    const customVisits = new Set(custom.map(([v]) => v.id));
    yield { sessions, events, eventData, umami: umamiStats(visits, customVisits) };
  }
}

function exportHostname(data: Record<Prefix, Row[]>): string | null {
  return data.pages.find((r) => r.hostname)?.hostname ?? null;
}

// ----------------------------------------------------------------------------- DB

const SESSION_COLS = [
  'session_id',
  'website_id',
  'browser',
  'os',
  'device',
  'screen',
  'language',
  'country',
  'region',
  'city',
  'distinct_id',
  'created_at',
];
const EVENT_COLS = [
  'event_id',
  'website_id',
  'session_id',
  'visit_id',
  'created_at',
  'url_path',
  'url_query',
  'utm_source',
  'utm_medium',
  'utm_campaign',
  'utm_content',
  'utm_term',
  'referrer_path',
  'referrer_query',
  'referrer_domain',
  'event_type',
  'event_name',
  'tag',
  'hostname',
];
const DATA_COLS = [
  'event_data_id',
  'website_id',
  'website_event_id',
  'data_key',
  'string_value',
  'data_type',
  'created_at',
];

// Postgres protocol limit: 65535 parameters per query
const MAX_PARAMS = 65535;

async function insertRows(client: pg.PoolClient, table: string, cols: string[], rows: DbRow[]) {
  const batch = Math.floor(MAX_PARAMS / cols.length);
  for (let i = 0; i < rows.length; i += batch) {
    const params: unknown[] = [];
    const tuples = rows.slice(i, i + batch).map(
      (r) =>
        `(${r
          .map((v) => {
            params.push(v);
            return `$${params.length}`;
          })
          .join(',')})`,
    );
    await client.query(
      `INSERT INTO ${table} (${cols.join(',')}) VALUES ${tuples.join(',')}`,
      params,
    );
  }
}

// order matters: event_data and session are found via website_event, so events are deleted last
async function deleteTagged(
  client: pg.PoolClient,
): Promise<[events: number, sessions: number, data: number]> {
  const d = await client.query(
    `DELETE FROM event_data WHERE website_event_id IN
       (SELECT event_id FROM website_event WHERE website_id = $1 AND tag = $2)`,
    [websiteId, TAG],
  );
  const s = await client.query(
    `DELETE FROM session WHERE session_id IN
       (SELECT session_id FROM website_event WHERE website_id = $1 AND tag = $2)`,
    [websiteId, TAG],
  );
  const e = await client.query(`DELETE FROM website_event WHERE website_id = $1 AND tag = $2`, [
    websiteId,
    TAG,
  ]);
  return [e.rowCount ?? 0, s.rowCount ?? 0, d.rowCount ?? 0];
}

// relational branch of Umami's getWebsiteStats (src/queries/sql/getWebsiteStats.ts)
// without the date filter
const STATS_SQL = `
select coalesce(sum(t.c), 0)::bigint as pageviews, count(distinct t.session_id)::int as visitors,
  count(distinct t.visit_id)::int as visits,
  coalesce(sum(case when t.c = 1 and t.has_custom_event = 0 then 1 else 0 end), 0)::int as bounces,
  coalesce(sum(extract(epoch from t.max_time - t.min_time)), 0)::bigint as totaltime
from (select session_id, visit_id,
    sum(case when event_type not in (2, 5) then 1 else 0 end) c,
    min(case when event_type not in (2, 5) then created_at end) min_time,
    max(case when event_type not in (2, 5) then created_at end) max_time,
    max(case when event_type = 2 then 1 else 0 end) has_custom_event
  from website_event where website_id = $1 and tag = $2 group by 1, 2
  having sum(case when event_type not in (2, 5) then 1 else 0 end) > 0) t`;

// ----------------------------------------------------------------------------- report

// sessions are generated per day, so visitors can simply be summed across days
function umamiStats(visits: Visit[], customVisits: Set<string>): Stats {
  let bounces = 0;
  let totaltime = 0;
  for (const v of visits) {
    if (v.pageviews === 1 && !customVisits.has(v.id)) bounces++;
    totaltime += (v.times[v.times.length - 1] - v.times[0]) / 1000;
  }
  return {
    pageviews: sumBy(visits, (v) => v.pageviews),
    visitors: new Set(visits.map((v) => v.session!.id)).size,
    visits: visits.length,
    bounces,
    totaltime: Math.round(totaltime),
  };
}

function addStats(a: Stats, b: Stats): Stats {
  return {
    pageviews: a.pageviews + b.pageviews,
    visitors: a.visitors + b.visitors,
    visits: a.visits + b.visits,
    bounces: a.bounces + b.bounces,
    totaltime: a.totaltime + b.totaltime,
  };
}

const ZERO_STATS: Stats = { pageviews: 0, visitors: 0, visits: 0, bounces: 0, totaltime: 0 };

function printCompare(title: string, exp: Stats, got: Stats) {
  console.log(`\n${title}`);
  console.log('  metric        plausible      umami');
  const rows: [string, number, number][] = [
    ['visitors', exp.visitors, got.visitors],
    ['visits', exp.visits, got.visits],
    ['pageviews', exp.pageviews, got.pageviews],
    ['bounces', exp.bounces, got.bounces],
    [
      'bounce %',
      +((100 * exp.bounces) / exp.visits).toFixed(1),
      +((100 * got.bounces) / got.visits).toFixed(1),
    ],
    ['avg visit s', Math.round(exp.totaltime / exp.visits), Math.round(got.totaltime / got.visits)],
  ];
  for (const [k, a, b] of rows) {
    console.log(
      `  ${k.padEnd(12)} ${String(a).padStart(10)} ${String(b).padStart(10)}${a === b ? '' : '   *'}`,
    );
  }
}

// ----------------------------------------------------------------------------- main

async function rollback(pool: pg.Pool) {
  const client = await pool.connect();
  try {
    if (dryRun) {
      const r = await client.query<{ n: number }>(
        `SELECT count(*)::int n FROM website_event WHERE website_id = $1 AND tag = $2`,
        [websiteId, TAG],
      );
      console.log(
        `[dry-run] would delete ${r.rows[0].n} events tagged '${TAG}' (+ their sessions and event_data)`,
      );
      return;
    }
    await client.query('BEGIN');
    const [e, s, d] = await deleteTagged(client);
    await client.query('COMMIT');
    console.log(`Rolled back: ${e} events, ${s} sessions, ${d} event_data rows`);
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function resolveCities(locations: Row[]): Promise<Map<string, string>> {
  const visitorsById = new Map<string, number>();
  for (const r of locations) {
    if (r.city && r.city !== '0') {
      visitorsById.set(r.city, (visitorsById.get(r.city) ?? 0) + toInt(r.visitors));
    }
  }
  const names = await resolveGeonames(new Set(visitorsById.keys()));
  const missing = [...visitorsById].filter(([id]) => !names.has(id)).sort((a, b) => b[1] - a[1]);
  if (missing.length) {
    console.warn(
      `WARNING: ${missing.length} city IDs not found in GeoNames (${sumBy(missing, ([, n]) => n)} visitors), city left empty:`,
    );
    for (const [id, n] of missing) console.warn(`  ${id}: ${n} visitors`);
  }
  return names;
}

/** Pre-write website checks; returns the number of events from a previous import */
async function checkWebsite(
  client: pg.PoolClient,
  hostname: string | null,
  rangeStart: Date,
  rangeEnd: Date,
): Promise<number> {
  const w = await client.query<{
    name: string;
    domain: string | null;
    reset_at: Date | null;
    deleted_at: Date | null;
  }>(`SELECT name, domain, reset_at, deleted_at FROM website WHERE website_id = $1`, [websiteId]);
  const site = w.rows[0] ?? die(`website ${websiteId} not found in this Umami database`);
  if (site.deleted_at) die(`website ${websiteId} is deleted`);
  console.log(`Umami:    "${site.name}" (${site.domain})`);
  const bare = (host: string) => host.replace(/^www\./, '');
  if (site.domain && hostname && bare(site.domain) !== bare(hostname)) {
    console.warn(`WARNING: website domain '${site.domain}' != export hostname '${hostname}'`);
  }
  // Umami doesn't show data before reset_at
  if (site.reset_at && site.reset_at > rangeStart) {
    console.warn(
      `WARNING: website reset_at=${site.reset_at.toISOString()} hides data before it; clear reset_at to see the import`,
    );
  }

  const t = await client.query<{ n: number }>(
    `SELECT count(*)::int n FROM website_event WHERE website_id = $1 AND tag = $2`,
    [websiteId, TAG],
  );
  const previous = t.rows[0].n;
  if (previous && !opt.replace) {
    die(`${previous} events tagged '${TAG}' already exist — use --replace or --rollback`);
  }
  if (previous)
    console.log(`--replace: ${previous} previously imported events will be deleted first`);

  const ov = await client.query<{ n: number; mn: Date | null; mx: Date | null }>(
    `SELECT count(*)::int n, min(created_at) mn, max(created_at) mx FROM website_event
     WHERE website_id = $1 AND created_at >= $2 AND created_at < $3 AND tag IS DISTINCT FROM $4`,
    [websiteId, rangeStart, rangeEnd, TAG],
  );
  const overlap = ov.rows[0];
  if (overlap.n) {
    const msg = `website already has ${overlap.n} live events in the import range (${overlap.mn?.toISOString()} .. ${overlap.mx?.toISOString()}); set --date-to to the day before`;
    if (!opt['allow-overlap']) die(`${msg}, or pass --allow-overlap`);
    console.warn(`WARNING: ${msg}`);
  }
  return previous;
}

async function runImport(pool: pg.Pool | null) {
  const exportDir = positionals[0] ?? die('export folder is required as the first argument');
  const tz = opt.tz ?? die('--tz is required (Plausible site timezone, e.g. Europe/Moscow)');
  try {
    new Intl.DateTimeFormat('en', { timeZone: tz });
  } catch {
    die(`unknown timezone: ${tz}`);
  }

  const data = await readExport(exportDir);
  const days = selectDays(data);
  const daySet = new Set(days);
  const hostname = exportHostname(data);
  const firstDay = days[0];
  const lastDay = days[days.length - 1];
  const rangeStart = new Date(localMidnight(firstDay, tz));
  const rangeEnd = new Date(localMidnight(nextDay(lastDay), tz));

  console.log(`Export:   ${exportDir}`);
  console.log(`Website:  ${websiteId}   hostname: ${hostname}   tz: ${tz}`);
  console.log(
    `Range:    ${firstDay} .. ${lastDay} (${days.length} days) = ${rangeStart.toISOString()} .. ${rangeEnd.toISOString()}`,
  );

  const dayTotals = data.visitors.filter((r) => daySet.has(r.date));
  const expected: Stats = {
    visitors: sumBy(dayTotals, (r) => toInt(r.visitors)),
    visits: sumBy(dayTotals, (r) => toInt(r.visits)),
    pageviews: sumBy(dayTotals, (r) => toInt(r.pageviews)),
    bounces: sumBy(dayTotals, (r) => toInt(r.bounces)),
    totaltime: sumBy(dayTotals, (r) => toInt(r.visit_duration)),
  };

  const cityNames = await resolveCities(data.locations.filter((r) => daySet.has(r.date)));

  const client = pool ? await pool.connect() : null;
  try {
    const previous = client ? await checkWebsite(client, hostname, rangeStart, rangeEnd) : 0;
    const write = client && !dryRun;
    if (write) {
      await client.query('BEGIN');
      if (previous) {
        const [e, s, d] = await deleteTagged(client);
        console.log(`Deleted previous import: ${e} events, ${s} sessions, ${d} event_data`);
      }
    }

    // website_id in the seed: importing the same export into another site gives different ids
    const rng = new Rng(`1:${websiteId}`);
    const notes: Notes = { customEvents: 0, nonbounce1pv: 0, poolPadded: 0 };
    const counts = { sessions: 0, events: 0, eventData: 0 };
    let umami = ZERO_STATS;
    let done = 0;
    for (const rows of generateDays(data, days, cityNames, rng, tz, notes)) {
      if (write) {
        await insertRows(client, 'session', SESSION_COLS, rows.sessions);
        await insertRows(client, 'website_event', EVENT_COLS, rows.events);
        await insertRows(client, 'event_data', DATA_COLS, rows.eventData);
      }
      counts.sessions += rows.sessions.length;
      counts.events += rows.events.length;
      counts.eventData += rows.eventData.length;
      umami = addStats(umami, rows.umami);
      done++;
      if (process.stdout.isTTY) {
        process.stdout.write(
          `\r${write ? 'Inserting' : 'Generating'}: ${done}/${days.length} days`,
        );
      }
    }
    if (process.stdout.isTTY) process.stdout.write('\n');

    console.log(
      `Rows:     ${counts.sessions} sessions, ${counts.events} events (${notes.customEvents} custom), ${counts.eventData} event_data`,
    );
    if (notes.nonbounce1pv) {
      console.log(
        `Note:     ${notes.nonbounce1pv} non-bounced 1-pageview visits in Plausible (resolved via custom events where possible)`,
      );
    }
    if (notes.poolPadded) {
      console.log(
        `Note:     ${notes.poolPadded} pageviews padded with the top page (per-page pool was short)`,
      );
    }
    printCompare('Expected result (Umami definitions, computed in memory):', expected, umami);

    if (!write) {
      console.log(
        client
          ? '\n[dry-run] all checks passed, nothing written.'
          : '\n[dry-run] UMAMI_DATABASE_URL is not set: DB checks skipped, nothing written.',
      );
      return;
    }

    const got = (await client.query<Stats>(STATS_SQL, [websiteId, TAG])).rows[0];
    await client.query('COMMIT');
    console.log('\nCommitted.');
    // bigint comes from pg as a string
    printCompare('Verification (queried from the Umami DB):', expected, {
      ...got,
      pageviews: Number(got.pageviews),
      totaltime: Number(got.totaltime),
    });
  } catch (err) {
    await client?.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client?.release();
  }
}

async function main() {
  if (!databaseUrl && !dryRun)
    die('UMAMI_DATABASE_URL is required (only --dry-run can run without a DB)');
  const pool = databaseUrl ? new pg.Pool({ connectionString: databaseUrl, max: 1 }) : null;
  try {
    if (opt.rollback) {
      if (!pool) die('--rollback needs UMAMI_DATABASE_URL');
      await rollback(pool);
    } else {
      await runImport(pool);
    }
  } finally {
    await pool?.end();
  }
}

await main();
