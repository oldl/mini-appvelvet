#!/usr/bin/env node
/**
 * SunSpots — Run this locally
 * 1. node run-pipeline.js setup     → crée la table Airtable
 * 2. node run-pipeline.js migrate   → pousse les 70 terrasses curées
 * 3. node run-pipeline.js harvest   → harvest Overpass → Airtable
 *
 * Requires: Node 18+
 */

const TOKEN      = process.env.AIRTABLE_TOKEN;
const BASE       = 'appLZBC3hd6wuUTVR';
const TABLE      = 'terrasses';
const AT_API     = `https://api.airtable.com/v0`;
const OVERPASS   = 'https://overpass-api.de/api/interpreter';
const NOMINATIM  = 'https://nominatim.openstreetmap.org/reverse';

const CITIES = {
  bruxelles: { label:'Bruxelles', lat:50.8503, lng:4.3517, radius:8000 },
  gand:      { label:'Gand',      lat:51.0543, lng:3.7174, radius:6000 },
  liege:     { label:'Liège',     lat:50.6292, lng:5.5796, radius:6000 },
  paris:     { label:'Paris',     lat:48.8566, lng:2.3522, radius:10000 },
  amsterdam: { label:'Amsterdam', lat:52.3676, lng:4.9041, radius:8000 },
};

const CURATED = [
  {name:"La Machine",             address:"Place Saint-Géry 2, 1000",                   city:"bruxelles",lat:50.8481,lng:4.3460},
  {name:"Le Grand Café",          address:"Bd Anspach 78, 1000",                        city:"bruxelles",lat:50.8490,lng:4.3488},
  {name:"Scott's Bar",            address:"Rue Montagne aux Herbes Potagères 2, 1000",  city:"bruxelles",lat:50.8484,lng:4.3524},
  {name:"Zebra",                  address:"Place Saint-Géry 33, 1000",                  city:"bruxelles",lat:50.8479,lng:4.3455},
  {name:"Café Walvis",            address:"Rue Antoine Dansaert 209, 1000",             city:"bruxelles",lat:50.8527,lng:4.3431},
  {name:"Bélier",                 address:"Borgwal 9, 1000",                            city:"bruxelles",lat:50.8486,lng:4.3469},
  {name:"The Purple Rose",        address:"Rue du Marché aux Herbes 97, 1000",          city:"bruxelles",lat:50.8483,lng:4.3530},
  {name:"The Wild Geese",         address:"Avenue Livingstone 2, 1000",                 city:"bruxelles",lat:50.8430,lng:4.3715},
  {name:"Bar du Canal",           address:"Rue Antoine Dansaert 208, 1000",             city:"bruxelles",lat:50.8558,lng:4.3425},
  {name:"Vertigo",                address:"Rue de Rollebeek 7, 1000",                   city:"bruxelles",lat:50.8444,lng:4.3545},
  {name:"Affligem Cafe",          address:"Bd Anspach 81, 1000",                        city:"bruxelles",lat:50.8488,lng:4.3489},
  {name:"Le Perroquet",           address:"Rue Watteeu 31, 1000",                       city:"bruxelles",lat:50.8437,lng:4.3557},
  {name:"Flamingo",               address:"Rue de Laken 177, 1000",                     city:"bruxelles",lat:50.8595,lng:4.3490},
  {name:"Arthur Orlans",          address:"Rue Antoine Dansaert 67, 1000",              city:"bruxelles",lat:50.8510,lng:4.3435},
  {name:"Au Vieux Saint-Antoine", address:"Rue du Marché au Charbon 25, 1000",          city:"bruxelles",lat:50.8469,lng:4.3476},
  {name:"Mappa Mundo",            address:"Rue du Pont de la Carpe 2, 1000",            city:"bruxelles",lat:50.8478,lng:4.3456},
  {name:"Delirium Café",          address:"Impasse de la Fidélité 4, 1000",             city:"bruxelles",lat:50.8490,lng:4.3527},
  {name:"Delirium Monasterium",   address:"Rue des Bouchers 10, 1000",                  city:"bruxelles",lat:50.8484,lng:4.3528},
  {name:"Marcelle",               address:"Place du Samedi 15, 1000",                   city:"bruxelles",lat:50.8564,lng:4.3457},
  {name:"La Fleur en Papier Doré",address:"Rue des Alexiens 55, 1000",                  city:"bruxelles",lat:50.8443,lng:4.3515},
  {name:"Café Floréo",            address:"Rue des Riches Claires 19, 1000",            city:"bruxelles",lat:50.8502,lng:4.3463},
  {name:"Barbeton",               address:"Rue Antoine Dansaert 114, 1000",             city:"bruxelles",lat:50.8522,lng:4.3423},
  {name:"Café Novo",              address:"Place de la Vieille Halle aux Blés 37, 1000",city:"bruxelles",lat:50.8463,lng:4.3517},
  {name:"La Pharmacie Anglaise",  address:"Coudenberg 66, 1000",                        city:"bruxelles",lat:50.8448,lng:4.3611},
  {name:"Cartagena Salsa Bar",    address:"Rue du Marché au Charbon 70, 1000",          city:"bruxelles",lat:50.8463,lng:4.3478},
  {name:"Little Delirium",        address:"Rue du Marché aux Fromages 9, 1000",         city:"bruxelles",lat:50.8486,lng:4.3524},
  {name:"Bier Circus",            address:"Rue de l'Enseignement 57, 1000",             city:"bruxelles",lat:50.8520,lng:4.3600},
  {name:"Cobra bar-gallery",      address:"Rue des Chartreux 1, 1000",                  city:"bruxelles",lat:50.8490,lng:4.3455},
  {name:"Grap Wine Bar",          address:"Rue Wappers 15, 1000",                       city:"bruxelles",lat:50.8450,lng:4.3540},
  {name:"Mezzo",                  address:"Borgwal 18, 1000",                           city:"bruxelles",lat:50.8485,lng:4.3469},
  {name:"Che Habana Café",        address:"Rue des Harengs 16, 1000",                   city:"bruxelles",lat:50.8487,lng:4.3530},
  {name:"Monk",                   address:"Rue Sainte-Catherine 42, 1000",              city:"bruxelles",lat:50.8508,lng:4.3472},
  {name:"La Brocante",            address:"Rue Blaes 170, 1000",                        city:"bruxelles",lat:50.8411,lng:4.3498},
  {name:"Jungle Bar",             address:"Rue des Pierres 52, 1000",                   city:"bruxelles",lat:50.8494,lng:4.3523},
  {name:"LOFT",                   address:"Rue de Namur 51, 1000",                      city:"bruxelles",lat:50.8448,lng:4.3610},
  {name:"WASBAR",                 address:"Rue Henri Maus 45, 1000",                    city:"bruxelles",lat:50.8475,lng:4.3498},
  {name:"La Lunette",             address:"Place de la Monnaie 3, 1000",                city:"bruxelles",lat:50.8517,lng:4.3555},
  {name:"Guingette Maurice",      address:"Parc du Cinquantenaire, 1000",               city:"bruxelles",lat:50.8415,lng:4.3946},
  {name:"Le Jardin Rooftop",      address:"Rue du Marché aux Poulets 7, 1000",          city:"bruxelles",lat:50.8502,lng:4.3525,rooftop:true},
  {name:"Le COOP",                address:"Avenue du Port 86C, 1000",                   city:"bruxelles",lat:50.8630,lng:4.3380,rooftop:true},
  {name:"Cospaia Rooftop",        address:"Rue de la Régence 2, 1000",                  city:"bruxelles",lat:50.8418,lng:4.3602,rooftop:true},
  {name:"Le Tavernier",           address:"Chaussée de Boondael 445, 1050",             city:"bruxelles",lat:50.8117,lng:4.3869},
  {name:"Victoria Bar",           address:"Avenue de la Toison d'Or 11, 1050",          city:"bruxelles",lat:50.8353,lng:4.3641},
  {name:"Wine Bar Mouchart",      address:"Rue Eugène Cattoir 11, 1050",                city:"bruxelles",lat:50.8219,lng:4.3765},
  {name:"Loui Bar & Restaurant",  address:"Avenue Louise 71, 1050",                     city:"bruxelles",lat:50.8345,lng:4.3624},
  {name:"Alice Cocktail Bar",     address:"Avenue Louise 190, 1050",                    city:"bruxelles",lat:50.8296,lng:4.3708},
  {name:"De Haus",                address:"Chaussée d'Ixelles 183, 1050",               city:"bruxelles",lat:50.8271,lng:4.3784},
  {name:"L'Amour Fou",            address:"Chaussée d'Ixelles 185, 1050",               city:"bruxelles",lat:50.8271,lng:4.3785},
  {name:"O Artista",              address:"Rue de la Brasserie 98A, 1050",              city:"bruxelles",lat:50.8280,lng:4.3820},
  {name:"Choco Bar",              address:"Chaussée d'Ixelles 149, 1050",               city:"bruxelles",lat:50.8280,lng:4.3773},
  {name:"Tapas Y Mas",            address:"Chaussée de Boondael 372, 1050",             city:"bruxelles",lat:50.8138,lng:4.3858},
  {name:"Etiquette Wines",        address:"Avenue Emile De Mot 19, 1050",               city:"bruxelles",lat:50.8252,lng:4.3770},
  {name:"Café Flora",             address:"Parvis de Saint-Gilles 16A, 1060",           city:"bruxelles",lat:50.8333,lng:4.3441},
  {name:"Brasserie de l'Union",   address:"Parvis de Saint-Gilles 55, 1060",            city:"bruxelles",lat:50.8334,lng:4.3438},
  {name:"Maison du Peuple",       address:"Parvis Saint-Gilles 39, 1060",               city:"bruxelles",lat:50.8330,lng:4.3435},
  {name:"Le Dillens",             address:"Place Julien Dillens 11, 1060",              city:"bruxelles",lat:50.8313,lng:4.3433},
  {name:"La Trinquette",          address:"Rue de l'Aqueduc 3, 1060",                   city:"bruxelles",lat:50.8274,lng:4.3493},
  {name:"Rubis Wine Bar",         address:"Avenue Adolphe Demeur 34, 1060",             city:"bruxelles",lat:50.8303,lng:4.3473},
  {name:"Café La Pompe",          address:"Chaussée de Waterloo 211, 1060",             city:"bruxelles",lat:50.8262,lng:4.3520},
  {name:"Cauri Bar",              address:"Chaussée d'Alsemberg 163, 1190",             city:"bruxelles",lat:50.8148,lng:4.3367},
  {name:"Bar à Nelson",           address:"Avenue Wielemans Ceuppens 128, 1190",        city:"bruxelles",lat:50.8186,lng:4.3311},
  {name:"Wiels Terrasse",         address:"Avenue Van Volxem 354, 1190",                city:"bruxelles",lat:50.8218,lng:4.3322,rooftop:true},
  {name:"L'Amère à boire",        address:"Chaussée d'Alsemberg 1008, 1180",            city:"bruxelles",lat:50.7861,lng:4.3558},
  {name:"Taverne Brusilia",       address:"Avenue Louis Bertrand 127, 1030",            city:"bruxelles",lat:50.8674,lng:4.3812},
  {name:"Bar du Gaspi",           address:"Chaussée de Haecht 309, 1030",               city:"bruxelles",lat:50.8700,lng:4.3750},
  {name:"Le Barboteur",           address:"Avenue Louis Bertrand 23, 1030",             city:"bruxelles",lat:50.8621,lng:4.3778},
  {name:"New Majestic Bar",       address:"Chaussée de Louvain 397, 1030",              city:"bruxelles",lat:50.8712,lng:4.3830},
];

// ── UTILS ────────────────────────────────────────────────────────────────

const sleep = ms => new Promise(r => setTimeout(r, ms));

function bbox(lat, lng, r) {
  const d = r/111320, dL = d/Math.cos(lat*Math.PI/180);
  return { s:lat-d, n:lat+d, w:lng-dL, e:lng+dL };
}
function haversine(la1,ln1,la2,ln2) {
  const R=6371000,dL=(la2-la1)*Math.PI/180,dN=(ln2-ln1)*Math.PI/180;
  const a=Math.sin(dL/2)**2+Math.cos(la1*Math.PI/180)*Math.cos(la2*Math.PI/180)*Math.sin(dN/2)**2;
  return R*2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
}
function bearing(la1,ln1,la2,ln2) {
  const dL=(ln2-ln1)*Math.PI/180,la1r=la1*Math.PI/180,la2r=la2*Math.PI/180;
  const y=Math.sin(dL)*Math.cos(la2r),x=Math.cos(la1r)*Math.sin(la2r)-Math.sin(la1r)*Math.cos(la2r)*Math.cos(dL);
  return (Math.atan2(y,x)*180/Math.PI+360)%360;
}
function toXY(lat,lng,rla,rln){return{x:(lng-rln)*Math.cos(rla*Math.PI/180)*111320,y:(lat-rla)*111320};}
function ptSeg(px,py,ax,ay,bx,by){const dx=bx-ax,dy=by-ay,l2=dx*dx+dy*dy;if(!l2)return Math.sqrt((px-ax)**2+(py-ay)**2);const t=Math.max(0,Math.min(1,((px-ax)*dx+(py-ay)*dy)/l2));return Math.sqrt((px-ax-t*dx)**2+(py-ay-t*dy)**2);}
function azimuthFromRoads(lat,lng,segs){
  if(!segs.length)return null;
  let best=Infinity,bestAz=null;
  for(const s of segs){const a=toXY(s.la1,s.ln1,lat,lng),b=toXY(s.la2,s.ln2,lat,lng);const d=ptSeg(0,0,a.x,a.y,b.x,b.y);if(d<best){best=d;const rb=bearing(s.la1,s.ln1,s.la2,s.ln2);const cross=(b.x-a.x)*(0-a.y)-(b.y-a.y)*(0-a.x);const perp=cross>0?(rb+90)%360:(rb-90+360)%360;bestAz=Math.round((perp+180)%360);}}
  return bestAz;
}

// ── AIRTABLE ─────────────────────────────────────────────────────────────

const atHeaders = { Authorization:`Bearer ${TOKEN}`, 'Content-Type':'application/json' };

async function atGet(path) {
  const r = await fetch(`${AT_API}${path}`, { headers: atHeaders });
  return r.json();
}
async function atPost(path, body) {
  const r = await fetch(`${AT_API}${path}`, { method:'POST', headers:atHeaders, body:JSON.stringify(body) });
  return r.json();
}

async function getExistingOsmIds() {
  const ids = new Set(); let offset = null;
  do {
    const url = `${AT_API}/v0/${BASE}/${TABLE}?fields[]=osm_id${offset?'&offset='+offset:''}`;
    const r = await fetch(url, {headers:atHeaders}); const d = await r.json();
    (d.records||[]).forEach(r => r.fields.osm_id && ids.add(r.fields.osm_id));
    offset = d.offset||null;
  } while(offset);
  return ids;
}

async function pushRecords(records) {
  let pushed=0;
  for(let i=0;i<records.length;i+=10){
    const chunk=records.slice(i,i+10);
    const d = await atPost(`/v0/${BASE}/${TABLE}`, {records:chunk.map(r=>({fields:r}))});
    if(d.error){console.error('Airtable error:',d.error.message);break;}
    pushed+=chunk.length;
    process.stdout.write(`\r  ↑ ${pushed}/${records.length}`);
    await sleep(250);
  }
  console.log();
}

// ── SETUP : crée la table si elle n'existe pas ───────────────────────────

async function setup() {
  console.log('\n🔧 Setup — vérification de la base Airtable...');
  const meta = await atGet(`/meta/bases/${BASE}/tables`);
  if(meta.error){ console.error('Erreur meta:', meta.error); return; }
  const existing = meta.tables && meta.tables.find(t => t.name === TABLE);
  if(existing){
    console.log(`✓ Table "${TABLE}" existe déjà (${existing.id})`);
    return;
  }
  console.log(`  → Création de la table "${TABLE}"...`);
  const res = await atPost(`/meta/bases/${BASE}/tables`, {
    name: TABLE,
    fields: [
      {name:'name',           type:'singleLineText'},
      {name:'address',        type:'singleLineText'},
      {name:'city',           type:'singleLineText'},
      {name:'city_label',     type:'singleLineText'},
      {name:'lat',            type:'number', options:{precision:6}},
      {name:'lng',            type:'number', options:{precision:6}},
      {name:'azimuth',        type:'number', options:{precision:0}},
      {name:'azimuth_source', type:'singleLineText'},
      {name:'osm_id',         type:'singleLineText'},
      {name:'source',         type:'singleLineText'},
      {name:'status',         type:'singleLineText'},
      {name:'rooftop',        type:'checkbox', options:{icon:'check',color:'greenBright'}},
      {name:'outdoor_seating',type:'checkbox', options:{icon:'check',color:'greenBright'}},
      {name:'votes_sun',      type:'number', options:{precision:0}},
      {name:'votes_shade',    type:'number', options:{precision:0}},
    ]
  });
  if(res.error) console.error('Erreur création table:', res.error);
  else console.log('✅ Table créée:', res.id);
}

// ── MIGRATE ───────────────────────────────────────────────────────────────

async function migrate() {
  console.log(`\n📦 Migration de ${CURATED.length} terrasses curées...`);
  const records = CURATED.map(c => ({
    name:c.name, address:c.address||'', city:c.city, city_label:'Bruxelles',
    lat:c.lat, lng:c.lng, azimuth:null, azimuth_source:'missing', osm_id:'',
    source:'curated_manual', status:'verified', rooftop:c.rooftop||false, outdoor_seating:true,
    votes_sun:0, votes_shade:0,
  }));
  await pushRecords(records);
  console.log(`✅ ${records.length} terrasses migrées (status: verified)`);
}

// ── HARVEST ───────────────────────────────────────────────────────────────

async function harvest(cityKey) {
  const city = CITIES[cityKey];
  if(!city){console.error('Ville inconnue:', cityKey, '| Disponibles:', Object.keys(CITIES).join(', '));return;}
  console.log(`\n🌍 Harvest ${city.label}...`);

  const b = bbox(city.lat,city.lng,city.radius), bS=`${b.s},${b.w},${b.n},${b.e}`;
  const q = `[out:json][timeout:30];(node(${bS})[amenity~"cafe|bar|restaurant"][outdoor_seating=yes];way(${bS})[amenity~"cafe|bar|restaurant"][outdoor_seating=yes];way(${bS})[highway~"^(primary|secondary|tertiary|residential|unclassified|living_street|pedestrian|service)$"];);out geom;`;
  console.log('  → Overpass...');
  const res = await fetch(`${OVERPASS}?data=${encodeURIComponent(q)}`);
  const data = await res.json();

  const cafes=[],roads=[];
  for(const el of(data.elements||[])){
    const t=el.tags||{};
    if(['cafe','bar','restaurant'].includes(t.amenity)&&t.outdoor_seating==='yes'){
      const lat=el.lat??(el.center && el.center.lat)??(el.geometry?el.geometry.reduce((s,p)=>s+p.lat,0)/el.geometry.length:null);
      const lng=el.lon??(el.center && el.center.lon)??(el.geometry?el.geometry.reduce((s,p)=>s+p.lon,0)/el.geometry.length:null);
      if(lat&&lng&&t.name)cafes.push({name:t.name,lat,lng,address:[t['addr:street'],t['addr:housenumber']].filter(Boolean).join(' ')||null,osm_id:String(el.id)});
    }
    if(t.highway&&(el.geometry && el.geometry.length)>=2){const g=el.geometry;for(let i=0;i<g.length-1;i++)roads.push({la1:g[i].lat,ln1:g[i].lon,la2:g[i+1].lat,ln2:g[i+1].lon});}
  }
  console.log(`  ✓ ${cafes.length} cafés OSM, ${roads.length} segments de routes`);

  const existingIds = await getExistingOsmIds();
  const newCafes = cafes.filter(c=>!existingIds.has(c.osm_id));
  console.log(`  ✓ ${newCafes.length} nouveaux (${cafes.length-newCafes.length} déjà dans Airtable)`);
  if(!newCafes.length){console.log('  Rien à ajouter.');return;}

  console.log('  → Calcul azimuts...');
  const toInsert=[];
  for(let i=0;i<newCafes.length;i++){
    const c=newCafes[i];
    process.stdout.write(`\r  ${i+1}/${newCafes.length} — ${c.name.substring(0,30).padEnd(30)}`);
    const azimuth=azimuthFromRoads(c.lat,c.lng,roads);
    toInsert.push({name:c.name,address:c.address||'',city:cityKey,city_label:city.label,lat:c.lat,lng:c.lng,azimuth,azimuth_source:azimuth?'auto_osm':'missing',osm_id:c.osm_id,source:'osm_harvest',status:'pending',rooftop:false,outdoor_seating:true,votes_sun:0,votes_shade:0});
  }
  console.log();
  console.log(`  → Push vers Airtable...`);
  await pushRecords(toInsert);
  console.log(`✅ ${toInsert.length} terrasses ajoutées (status: pending — à valider dans Airtable)`);
}

// ── CLI ───────────────────────────────────────────────────────────────────

const cmd = process.argv[2] || 'help';
const cityArg = process.argv[3] || 'bruxelles';

(async () => {
  try {
    if(cmd==='setup')        await setup();
    else if(cmd==='migrate') await migrate();
    else if(cmd==='harvest') await harvest(cityArg);
    else if(cmd==='all') {
      await setup();
      await migrate();
      await harvest('bruxelles');
    }
    else {
      console.log(`
SunSpots Pipeline

Usage:
  node run-pipeline.js setup              → Crée la table Airtable
  node run-pipeline.js migrate            → Migre les 70 terrasses curées (status: verified)
  node run-pipeline.js harvest bruxelles  → Harvest OSM pour Bruxelles (status: pending)
  node run-pipeline.js harvest gand       → Harvest pour Gand
  node run-pipeline.js harvest paris      → Harvest pour Paris
  node run-pipeline.js all                → Setup + migrate + harvest Bruxelles en une fois

Villes disponibles: ${Object.keys(CITIES).join(', ')}
      `);
    }
  } catch(e) {
    console.error('Erreur:', e.message);
    process.exit(1);
  }
})();
