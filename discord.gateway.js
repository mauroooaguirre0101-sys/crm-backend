'use strict';
const { Client, GatewayIntentBits } = require('discord.js');

let _client = null;

function startGateway() {
  if (!process.env.DISCORD_BOT_TOKEN) {
    console.log('⚠️  Discord gateway: DISCORD_BOT_TOKEN missing, bot will stay offline');
    return;
  }

  // Guilds is the only intent needed — bot sends messages, never reads them
  _client = new Client({
    intents: [GatewayIntentBits.Guilds],
  });

  _client.once('ready', () => {
    console.log(`✅ Discord bot connected as ${_client.user.tag} (${_client.user.id})`);
  });

  _client.on('error', (err) => {
    console.error('Discord gateway error:', err.message);
  });

  // discord.js reconnects automatically — no manual handling needed
  _client.login(process.env.DISCORD_BOT_TOKEN).catch((err) => {
    console.error('Discord login failed:', err.message);
  });
}

module.exports = { startGateway };
