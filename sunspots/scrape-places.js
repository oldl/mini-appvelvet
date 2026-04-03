#!/usr/bin/env node
// scrape-places.js
// Enrichit les terrasses SunSpots avec Google Places API (New) + Airtable
// Usage:
//   GOOGLE_PLACES_API_KEY=xxx AIRTABLE_TOKEN=xxx AIRTABLE_BASE_ID=app... AIRTABLE_TABLE_ID=tbl... node scrape-places.js
// Options:
//   DRY_RUN=1   -> ne patch pas Airtable
//   LIMIT=20    -> limite le nombre d'enregistrements traites

const https = require('https');

const GKEY = process.env.GOOGLE_PLACES_API_KEY;
const AT_TOKEN = process.env.AIRTABLE_TOKEN;
const BASE = process.env.AIRTABLE_BASE_ID;
const TABLE = process.env.AIRTABLE_TABLE_ID;
const DRY_RUN = process.env.DRY_RUN === '1';
const LIMIT = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : null;

if (!GKEY || !AT_TOKEN || !BASE || !TABLE) {
  console.error('Missing required env vars: GOOGLE_PLACES_API_KEY, AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID');
  process.exit(1);
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

function requestJson({ hostname, path, method = 'GET', headers = {}, body = null }) {
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path, method, headers }, res => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          const json = data ? JSON.parse(data) : {};
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve(json);
          } else {
            reject(new Error(`HTTP ${res.statusCode} ${method} ${hostname}${path} :: ${JSON.stringify(json)}`));
          }
        } catch (e) {
          reject(new Error(`Invalid JSON response from ${hostname}${path}: ${e.message}\nRaw: ${data.slice(0, 500)}`));
        }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

function slugify(str) {
  return String(str || '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
}

async function airtableGet(path) {
  return requestJson({
    hostname: 'api.airtable.com',
    path,
    method: 'GET',
    headers: {
      Authorization: `Bearer ${AT_TOKEN}`
    }
  });
}

async function airtablePatch(recordId, fields) {
  const body = JSON.stringify({ fields });
  return requestJson({
    hostname: 'api.airtable.com',
    path: `/v0/${BASE}/${TABLE}/${recordId}`,
    method: 'PATCH',
    headers: {
      Authorization: `Bearer ${AT_TOKEN}`,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body)
    },
    body
  });
}

async function getAllRecords() {
  const fields = ['Name', 'address', 'lat', 'lng', 'placeId'];
  let records = [];
  let offset = null;

  do {
    let path = `/v0/${BASE}/${TABLE}?pageSize=100`;
    for (const f of fields) path += `&fields[]=${encodeURIComponent(f)}`;
    if (offset) path += `&offset=${encodeURIComponent(offset)}`;

    const res = await airtableGet(path);
    records = records.concat(res.records || []);
    offset = res.offset || null;
    process.stdout.write(`Fetched ${records.length} Airtable records\r`);
  } while (offset);

  console.log('');
  return LIMIT ? records.slice(0, LIMIT) : records;
}

async function searchPlace(name, address, lat, lng) {
  const textQuery = [name, address, 'Brussels'].filter(Boolean).join(' ');
  const body = JSON.stringify({
    textQuery,
    locationBias: (lat && lng) ? {
      circle: {
        center: { latitude: Number(lat), longitude: Number(lng) },
        radius: 500
      }
    } : undefined
  });

  const res = await requestJson({
    hostname: 'places.googleapis.com',
    path: '/v1/places:searchText',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Goog-Api-Key': GKEY,
      'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress'
    },
    body
  });

  if (res.places && res.places[0] && res.places[0].id) {
    return res.places[0].id;
  }
  return null;
}

async function getPlaceDetails(placeId) {
  return requestJson({
    hostname: 'places.googleapis.com',
    path: `/v1/places/${encodeURIComponent(placeId)}`,
    method: 'GET',
    headers: {
      'X-Goog-Api-Key': GKEY,
      'X-Goog-FieldMask': [
        'id',
        'displayName',
        'websiteUri',
        'rating',
        'userRatingCount',
        'regularOpeningHours',
        'googleMapsUri'
      ].join(',')
    }
  });
}

function mapDetailsToFields(details, existingPlaceId) {
  const fields = {};

  if (details.websiteUri) {
    fields.website = details.websiteUri;
  }

  if (details.regularOpeningHours?.weekdayDescriptions?.length) {
    fields.openingHoursText =
      details.regularOpeningHours.weekdayDescriptions.join(' | ');
  }

  if (typeof details.rating === 'number') {
    fields.note = Number(details.rating.toFixed(1));
  }

  if (typeof details.userRatingCount === 'number') {
    fields.nombre_avis = details.userRatingCount;
  }

  if (details.googleMapsUri) {
    fields.googleMapsUrl = details.googleMapsUri;
  }

  if (existingPlaceId == null && details.id) {
    fields.placeId = details.id;
  }

  return fields;
}

async function main() {
  console.log('Fetching Airtable records...');
  const records = await getAllRecords();
  console.log(`Loaded ${records.length} records`);

  let found = 0;
  let updated = 0;
  let notFound = 0;
  let errors = 0;
  let skipped = 0;

  for (let i = 0; i < records.length; i++) {
    const r = records[i];
    const f = r.fields || {};
    const name = f.Name;
    const address = f.address || '';
    const lat = f.lat;
    const lng = f.lng;
    let placeId = f.placeId || null;

    if (!name) {
      skipped++;
      console.log(`[${i + 1}/${records.length}] skipped: missing Name`);
      continue;
    }

    process.stdout.write(`[${i + 1}/${records.length}] ${String(name).slice(0, 36).padEnd(36)} `);

    try {
      if (!placeId) {
        placeId = await searchPlace(name, address, lat, lng);
        if (!placeId) {
          notFound++;
          process.stdout.write('— not found\n');
          await sleep(120);
          continue;
        }
      }

      found++;
      const details = await getPlaceDetails(placeId);
      const fields = mapDetailsToFields(details, f.placeId);

      if (!f.placeId && placeId) fields.placeId = placeId;
      if (!f.slug && name) fields.slug = slugify(name);

      if (Object.keys(fields).length === 0) {
        process.stdout.write('— no new data\n');
        await sleep(120);
        continue;
      }

      if (DRY_RUN) {
        updated++;
        process.stdout.write(`DRY_RUN ${JSON.stringify(fields)}\n`);
      } else {
        await airtablePatch(r.id, fields);
        updated++;
        const info = [
          fields.note ? `⭐${fields.note}${fields.nombre_avis ? `(${fields.nombre_avis})` : ''}` : '',
          fields.website ? '🌐' : '',
          fields.openingHoursText ? '🕐' : '',
          fields.placeId ? '📍' : ''
        ].filter(Boolean).join(' ');
        process.stdout.write(`✅ ${info}\n`);
      }
    } catch (e) {
      errors++;
      process.stdout.write(`❌ ${e.message}\n`);
      await sleep(300);
      continue;
    }

    await sleep(120);
  }

  console.log('\n─────────────────────────────────');
  console.log('Done');
  console.log(`Found in Google: ${found}/${records.length}`);
  console.log(`Updated in AT:   ${updated}`);
  console.log(`Not found:       ${notFound}`);
  console.log(`Skipped:         ${skipped}`);
  console.log(`Errors:          ${errors}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
