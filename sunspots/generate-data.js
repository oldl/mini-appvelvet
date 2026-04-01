#!/usr/bin/env node
/**
 * SunSpots â generate-data.js
 * GÃ©nÃ¨re sunspots-data.json depuis Airtable
 * Usage: node generate-data.js
 * DÃ©pose le JSON gÃ©nÃ©rÃ© dans le mÃªme dossier que index.html sur Netlify
 */

const TOKEN = process.env.AIRTABLE_TOKEN
const BASE  = 'appLZBC3hd6wuUTVR';
const TABLE = 'tbl3wivJNAop2e1z7';
const fs    = require('fs');
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function fetchAllAirtable() {
  console.log('Fetching Airtable...');
  const params = new URLSearchParams({
    filterByFormula: '{city}="bruxelles"',
    pageSize: '100',
  });
['Name','address','lat','lng','azimuth','rooftop','street_type'].forEach(f => params.append('fields[]', f));
  let records = [], offset = null, page = 0;
  do {
    if (offset) params.set('offset', offset); else params.delete('offset');
    const res = await fetch(`https://api.airtable.com/v0/${BASE}/${TABLE}?${params}`, {
      headers: { Authorization: `Bearer ${TOKEN}` }
    });
    const d = await res.json();
    if (d.error) throw new Error(d.error.message);
    records = records.concat(d.records || []);
    offset = d.offset || null;
    page++;
    process.stdout.write(`\r  page ${page} â ${records.length} records`);
    await sleep(250);
  } while (offset);

  console.log(`\nâ ${records.length} records fetched`);
  return records;
}

const COMMUNES = {
  'ixelles':      { lat:50.8280, lng:4.3750, radius:900  },
  'saint-gilles': { lat:50.8330, lng:4.3460, radius:800  },
  'forest':       { lat:50.8120, lng:4.3340, radius:1000 },
  'uccle':        { lat:50.7980, lng:4.3570, radius:1200 },
  'centre':       { lat:50.8490, lng:4.3490, radius:900  },
  'schaerbeek':   { lat:50.8670, lng:4.3790, radius:900  },
  'etterbeek':    { lat:50.8360, lng:4.3930, radius:900  },
  'anderlecht':   { lat:50.8360, lng:4.3070, radius:1200 },
  'molenbeek':    { lat:50.8530, lng:4.3280, radius:900  },
  'jette':        { lat:50.8810, lng:4.3240, radius:900  },
  'saint-josse':  { lat:50.8620, lng:4.3680, radius:600  },
  'woluwe-sl':    { lat:50.8390, lng:4.4320, radius:1000 },
  'woluwe-sp':    { lat:50.8190, lng:4.4390, radius:1000 },
  'auderghem':    { lat:50.8100, lng:4.4180, radius:900  },
};

function haversine(la1,ln1,la2,ln2){
  const R=6371000,dL=(la2-la1)*Math.PI/180,dN=(ln2-ln1)*Math.PI/180;
  const a=Math.sin(dL/2)**2+Math.cos(la1*Math.PI/180)*Math.cos(la2*Math.PI/180)*Math.sin(dN/2)**2;
  return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
}

async function main() {
  const records = await fetchAllAirtable();

  // Normalize
  const all = records.map(r => ({
    name:    r.fields.Name,
    address: r.fields.address || null,
    lat:     Number(r.fields.lat),
    lng:     Number(r.fields.lng),
    azimuth: Number.isFinite(Number(r.fields.azimuth)) ? Number(r.fields.azimuth) : null,
    rooftop: Boolean(r.fields.rooftop),
street_type: r.fields.street_type || null,
  })).filter(t => Number.isFinite(t.lat) && Number.isFinite(t.lng) && t.name);
  
  console.log(`â ${all.length} valid records after normalization`);

  // Partition by commune
  const byCommune = {};
  for (const [key, c] of Object.entries(COMMUNES)) {
    const radius = c.radius * 2.5;
    byCommune[key] = all.filter(t => haversine(c.lat, c.lng, t.lat, t.lng) <= radius);
    console.log(`  ${key}: ${byCommune[key].length} spots`);
  }

  const output = {
    generated: new Date().toISOString(),
    total: all.length,
    bruxelles: byCommune,
  };

  const outPath = './sunspots/sunspots-data.json';
  fs.writeFileSync(outPath, JSON.stringify(output));
  const size = (fs.statSync(outPath).size / 1024).toFixed(1);
  console.log(`\nâ ${outPath} generated â ${size}kb`);
  console.log('   â DÃ©pose ce fichier avec index.html sur Netlify');
}

main().catch(e => { console.error('Error:', e.message); process.exit(1); });
