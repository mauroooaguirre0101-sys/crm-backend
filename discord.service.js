'use strict';
const DISCORD_API = 'https://discord.com/api/v10';

// VIEW_CHANNEL(1024) + SEND_MESSAGES(2048) + READ_MESSAGE_HISTORY(65536)
const STUDENT_ALLOW = '68608';
const VIEW_DENY     = '1024';

// Discord error code reference for permission debugging
const DISCORD_ERRORS = {
  '10003': 'Unknown Channel — DISCORD_CATEGORY_ID may be invalid or deleted',
  '10004': 'Unknown Guild — DISCORD_GUILD_ID is wrong',
  '50001': 'Missing Access — bot cannot access this guild or channel',
  '50013': 'Missing Permissions — bot needs Manage Channels (and Manage Roles for overwrites)',
  '50035': 'Invalid Form Body — channel name contains invalid characters or is too long',
  '30013': 'Maximum Channels reached — guild has hit the 500 channel limit',
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

// Add user to the main guild using their OAuth access_token (requires guilds.join scope)
async function addGuildMember(discordUserId, accessToken) {
  console.log(`[Discord] addGuildMember — user ${discordUserId} → guild ${process.env.DISCORD_GUILD_ID}`);
  try {
    const result = await _req('PUT', `/guilds/${process.env.DISCORD_GUILD_ID}/members/${discordUserId}`, {
      access_token: accessToken,
    });
    // null = 204 = already a member
    console.log(`[Discord] addGuildMember — ${result === null ? 'already a member' : 'joined successfully'}`);
  } catch (err) {
    const hint = _parseDiscordError(err.message);
    console.warn(`[Discord] addGuildMember warn: ${err.message}${hint ? ` → ${hint}` : ''}`);
  }
}

// Create a private text channel visible only to the student + admins
async function createPrivateChannel(rawName, discordUserId) {
  const guildId = process.env.DISCORD_GUILD_ID;
  const name = rawName.toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').slice(0, 100);

  console.log(`[Discord] createPrivateChannel — name="${name}" user=${discordUserId} guild=${guildId}`);
  console.log(`[Discord] Env check — CATEGORY_ID=${process.env.DISCORD_CATEGORY_ID || '(none)'} ADMIN_ROLE_ID=${process.env.DISCORD_ADMIN_ROLE_ID || '(none)'}`);

  const overwrites = [
    { id: guildId,       type: 0, deny:  VIEW_DENY     }, // deny @everyone VIEW_CHANNEL
    { id: discordUserId, type: 1, allow: STUDENT_ALLOW }, // allow student: view+send+history
  ];

  if (process.env.DISCORD_ADMIN_ROLE_ID) {
    overwrites.push({ id: process.env.DISCORD_ADMIN_ROLE_ID, type: 0, allow: STUDENT_ALLOW });
    console.log(`[Discord] Admin role overwrite added: ${process.env.DISCORD_ADMIN_ROLE_ID}`);
  }

  console.log(`[Discord] Permission overwrites: ${JSON.stringify(overwrites)}`);

  const body = { name, type: 0, permission_overwrites: overwrites };
  if (process.env.DISCORD_CATEGORY_ID) {
    body.parent_id = process.env.DISCORD_CATEGORY_ID;
    console.log(`[Discord] Placing channel under category: ${process.env.DISCORD_CATEGORY_ID}`);
  }

  try {
    const channel = await _req('POST', `/guilds/${guildId}/channels`, body);
    console.log(`[Discord] Channel created — id=${channel.id} name="${channel.name}" parent=${channel.parent_id || 'root'}`);
    return channel;
  } catch (err) {
    const hint = _parseDiscordError(err.message);
    console.error(`[Discord] createPrivateChannel FAILED: ${err.message}`);
    if (hint) console.error(`[Discord] Hint: ${hint}`);
    console.error(`[Discord] Debug — guild=${guildId} category=${process.env.DISCORD_CATEGORY_ID || '(none)'} adminRole=${process.env.DISCORD_ADMIN_ROLE_ID || '(none)'}`);
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
async function getGuildMember(discordUserId) {
  try {
    return await _req('GET', `/guilds/${process.env.DISCORD_GUILD_ID}/members/${discordUserId}`);
  } catch { return null; }
}

// Find an existing channel where discordUserId has an explicit overwrite (user's private channel)
async function findChannelByUser(discordUserId) {
  try {
    const channels = await _req('GET', `/guilds/${process.env.DISCORD_GUILD_ID}/channels`);
    if (!Array.isArray(channels)) return null;
    return channels.find(c =>
      c.type === 0 &&
      Array.isArray(c.permission_overwrites) &&
      c.permission_overwrites.some(o => o.id === discordUserId && o.type === 1)
    ) || null;
  } catch { return null; }
}

module.exports = { addGuildMember, createPrivateChannel, sendChannelMessage, sendEmbed, getGuildMember, findChannelByUser };
