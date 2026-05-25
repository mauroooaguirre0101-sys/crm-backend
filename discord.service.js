'use strict';
const DISCORD_API = 'https://discord.com/api/v10';

// VIEW_CHANNEL(1024) + SEND_MESSAGES(2048) + READ_MESSAGE_HISTORY(65536)
const STUDENT_ALLOW = '68608';
const VIEW_DENY     = '1024';

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
  try {
    await _req('PUT', `/guilds/${process.env.DISCORD_GUILD_ID}/members/${discordUserId}`, {
      access_token: accessToken,
    });
  } catch (err) {
    // Status 204 = already a member — that's fine
    if (!err.message.includes('204')) console.warn('addGuildMember warn:', err.message);
  }
}

// Create a private text channel visible only to the student + admins
async function createPrivateChannel(rawName, discordUserId) {
  const guildId = process.env.DISCORD_GUILD_ID;
  const name = rawName.toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '').slice(0, 100);

  const overwrites = [
    { id: guildId,       type: 0, deny:  VIEW_DENY     }, // deny @everyone
    { id: discordUserId, type: 1, allow: STUDENT_ALLOW }, // allow student
  ];

  // Optionally add an admin role
  if (process.env.DISCORD_ADMIN_ROLE_ID) {
    overwrites.push({ id: process.env.DISCORD_ADMIN_ROLE_ID, type: 0, allow: STUDENT_ALLOW });
  }

  const body = { name, type: 0, permission_overwrites: overwrites };
  if (process.env.DISCORD_CATEGORY_ID) body.parent_id = process.env.DISCORD_CATEGORY_ID;

  return _req('POST', `/guilds/${guildId}/channels`, body);
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

module.exports = { addGuildMember, createPrivateChannel, sendChannelMessage, sendEmbed, getGuildMember };
