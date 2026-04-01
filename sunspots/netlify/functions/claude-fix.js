// netlify/functions/claude-fix.js
// Receives a fix instruction from Make/Telegram
// → fetches index.html from GitHub
// → asks Claude for a TARGETED str_replace patch (not full file rewrite)
// → applies patch and pushes back to GitHub

const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const CLAUDE_KEY   = process.env.ANTHROPIC_API_KEY;
const REPO         = 'oldl/sunspots';
const FILE_PATH    = 'sunspots/index.html';
const BRANCH       = 'main';
const GH_API       = 'https://api.github.com';

exports.handler = async (event) => {
  const CORS = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };
  if (event.httpMethod !== 'POST') return { statusCode: 405, headers: CORS, body: JSON.stringify({ error: 'Method not allowed' }) };

  let instruction;
  try {
    const body = JSON.parse(event.body || '{}');
    instruction = body.instruction || body.text || '';
  } catch {
    return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Invalid JSON' }) };
  }

  if (!instruction) return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: 'Missing instruction' }) };
  if (!CLAUDE_KEY)  return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Missing ANTHROPIC_API_KEY' }) };
  if (!GITHUB_TOKEN) return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: 'Missing GITHUB_TOKEN' }) };

  try {
    // 1. Fetch current file
    const ghRes = await fetch(`${GH_API}/repos/${REPO}/contents/${FILE_PATH}?ref=${BRANCH}`, {
      headers: { Authorization: `token ${GITHUB_TOKEN}`, Accept: 'application/vnd.github.v3+json' }
    });
    if (!ghRes.ok) throw new Error(`GitHub fetch: ${(await ghRes.json()).message}`);
    const ghData = await ghRes.json();
    const currentContent = Buffer.from(ghData.content, 'base64').toString('utf-8');
    const fileSha = ghData.sha;

    // 2. Ask Claude for a str_replace patch ONLY — never the full file
    const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': CLAUDE_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 2048,
        messages: [{
          role: 'user',
          content: `Tu es un assistant qui modifie du code HTML par patches ciblés.

Instruction : ${instruction}

Voici le fichier HTML à modifier (${currentContent.length} chars) :
${currentContent}

Réponds UNIQUEMENT avec un objet JSON (pas de markdown, pas de backticks) ayant cette structure exacte :
{
  "old": "la chaîne EXACTE à remplacer (copiée mot pour mot depuis le fichier)",
  "new": "la nouvelle chaîne de remplacement"
}

Règles importantes :
- "old" doit être une chaîne unique dans le fichier (au moins 20 chars de contexte)
- Ne retourne QUE le JSON, rien d'autre
- Si l'instruction nécessite plusieurs changements, fais le plus important`
        }],
      }),
    });

    if (!claudeRes.ok) {
      const err = await claudeRes.json();
      throw new Error(`Claude API: ${err.error?.message}`);
    }

    const claudeData = await claudeRes.json();
    const rawResponse = claudeData.content?.[0]?.text || '';

    // 3. Parse the patch
    let patch;
    try {
      const clean = rawResponse.replace(/```json|```/g, '').trim();
      patch = JSON.parse(clean);
    } catch {
      throw new Error(`Claude returned invalid JSON: ${rawResponse.substring(0, 200)}`);
    }

    if (!patch.old || !patch.new) throw new Error('Claude patch missing old or new field');
    if (!currentContent.includes(patch.old)) throw new Error(`String not found in file: "${patch.old.substring(0, 80)}"`);

    // 4. Apply patch
    const newContent = currentContent.replace(patch.old, patch.new);
    if (newContent === currentContent) throw new Error('Patch produced no change');
    if (newContent.length < currentContent.length * 0.5) throw new Error('Patch would reduce file size by more than 50% — aborting');

    // 5. Push to GitHub
    const commitMsg = `fix(claude): ${instruction.replace(/^fix:\s*/i, '').trim().substring(0, 72)}`;
    const pushRes = await fetch(`${GH_API}/repos/${REPO}/contents/${FILE_PATH}`, {
      method: 'PUT',
      headers: { Authorization: `token ${GITHUB_TOKEN}`, Accept: 'application/vnd.github.v3+json', 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: commitMsg,
        content: Buffer.from(newContent).toString('base64'),
        sha: fileSha,
        branch: BRANCH,
      }),
    });

    if (!pushRes.ok) throw new Error(`GitHub push: ${(await pushRes.json()).message}`);
    const pushData = await pushRes.json();
    const commitSha = pushData.commit?.sha?.substring(0, 7);

    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({ success: true, message: `✅ Fix appliqué (${commitSha}) — Netlify redéploie dans ~10s`, commit: commitSha }),
    };

  } catch (err) {
    console.error('claude-fix error:', err.message);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ success: false, error: err.message }) };
  }
};
