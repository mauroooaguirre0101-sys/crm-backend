'use strict';
const DISCORD_API = 'https://discord.com/api/v10';

// VIEW_CHANNEL(1024) + SEND_MESSAGES(2048) + READ_MESSAGE_HISTORY(65536)
const STUDENT_ALLOW = '68608';
const VIEW_DENY     = '1024';

const DISCORD_ERRORS = {
  '10003': 'Unknown Channel — category_id o channel_id inválido o eliminado',
  '10004': 'Unknown Guild — guild_id incorrecto',
  '50001': 'Missing Access — el bot no tiene acceso al servidor o canal',
  '50013': 'Missing Permissions — el bot necesita Manage Channels y Manage Roles',
  '50035': 'Invalid Form Body — nombre de canal con caracteres inválidos o muy largo',
  '30013': 'Maximum Channels — el servidor alcanzó el límite de 500 canales',
};

function _parseDiscordError(errMsg) {
  for (const [code, hint] of Object.entries(DISCORD_ERRORS)) {
    if (errMsg.includes(code)) return hint;
  }
  return null;
}

async function _req(method, path, body = null) {
  if (!process.env.DISCORD_BOT_TOKEN) throw new Error('DISCORD_BOT_TOKEN not set');
  const opts = {
    method,
    headers: {
      Authorization: `Bot ${process.env.DISCORD_BOT_TOKEN}`,
      'Content-Type': 'application/json',
    },
  };
  if (body !== null) opts.body = JSON.stringify(body);
  const res = await fetch(`${DISCORD_API}${path}`, opts);
  if (res.status === 204) return null;
  const data = await res.json();
  if (!res.ok) throw new Error(`Discord ${method} ${path} [${res.status}]: ${JSON.stringify(data).slice(0, 300)}`);
  return data;
}

// Resolve guild config — per-client row takes priority, env vars are fallback
function _resolveGuild(cfg = {}) {
  return {
    guildId:    cfg.guild_id    || process.env.DISCORD_GUILD_ID    || null,
    categoryId: cfg.category_id || process.env.DISCORD_CATEGORY_ID || null,
    adminRole:  cfg.admin_role_id || process.env.DISCORD_ADMIN_ROLE_ID || null,
  };
}

// Add user to a guild using their OAuth access_token (requires guilds.join scope)
async function addGuildMember(discordUserId, accessToken, cfg = {}) {
  const { guildId } = _resolveGuild(cfg);
  if (!guildId) {
    console.warn('[Discord] addGuildMember — no guild_id configured, skipping');
    return;
  }
  console.log(`[Discord] addGuildMember — user=${discordUserId} guild=${guildId}`);
  try {
    const result = await _req('PUT', `/guilds/${guildId}/members/${discordUserId}`, {
      access_token: accessToken,
    });
    console.log(`[Discord] addGuildMember — ${result === null ? 'already a member' : 'joined successfully'}`);
  } catch (err) {
    const hint = _parseDiscordError(err.message);
    console.warn(`[Discord] addGuildMember warn: ${err.message}${hint ? ` → ${hint}` : ''}`);
  }
}

// Create a private text channel visible only to the student + admin role
async function createPrivateChannel(rawName, discordUserId, cfg = {}) {
  const { guildId, categoryId, adminRole } = _resolveGuild(cfg);
  if (!guildId) throw new Error('No guild_id configurado para este cliente');

  const name = rawName.toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').slice(0, 100);

  console.log(`[Discord] createPrivateChannel — cliente_id=${cfg.cliente_id || '?'} guild=${guildId} category=${categoryId || '(none)'} name="${name}" user=${discordUserId}`);

  const overwrites = [
    { id: guildId,       type: 0, deny:  VIEW_DENY     }, // @everyone: sin acceso
    { id: discordUserId, type: 1, allow: STUDENT_ALLOW }, // alumno: ver + escribir + historial
  ];
  if (adminRole) {
    overwrites.push({ id: adminRole, type: 0, allow: STUDENT_ALLOW });
    console.log(`[Discord] Admin role overwrite: ${adminRole}`);
  }

  const body = { name, type: 0, permission_overwrites: overwrites };
  if (categoryId) body.parent_id = categoryId;

  try {
    const channel = await _req('POST', `/guilds/${guildId}/channels`, body);
    console.log(`[Discord] Channel created — id=${channel.id} name="${channel.name}" parent=${channel.parent_id || 'root'}`);
    return channel;
  } catch (err) {
    const hint = _parseDiscordError(err.message);
    console.error(`[Discord] createPrivateChannel FAILED — guild=${guildId} category=${categoryId || '(none)'}: ${err.message}`);
    if (hint) console.error(`[Discord] Hint: ${hint}`);
    throw err;
  }
}

// Send a plain text message to a channel
async function sendChannelMessage(channelId, content) {
  if (!channelId || !content) return null;
  return _req('POST', `/channels/${channelId}/messages`, { content });
}

// Send a rich embed message to a channel
async function sendEmbed(channelId, embed) {
  if (!channelId || !embed) return null;
  return _req('POST', `/channels/${channelId}/messages`, { embeds: [embed] });
}

// Fetch guild member info (returns null if user not in guild)
async function getGuildMember(discordUserId, cfg = {}) {
  const { guildId } = _resolveGuild(cfg);
  if (!guildId) return null;
  try {
    return await _req('GET', `/guilds/${guildId}/members/${discordUserId}`);
  } catch { return null; }
}

// Find an existing channel where discordUserId has an explicit overwrite (anti-duplicate)
async function findChannelByUser(discordUserId, cfg = {}) {
  const { guildId } = _resolveGuild(cfg);
  if (!guildId) return null;
  try {
    const channels = await _req('GET', `/guilds/${guildId}/channels`);
    if (!Array.isArray(channels)) return null;
    return channels.find(c =>
      c.type === 0 &&
      Array.isArray(c.permission_overwrites) &&
      c.permission_overwrites.some(o => o.id === discordUserId && o.type === 1)
    ) || null;
  } catch { return null; }
}

module.exports = {
  addGuildMember,
  createPrivateChannel,
  sendChannelMessage,
  sendEmbed,
  getGuildMember,
  findChannelByUser,
};
