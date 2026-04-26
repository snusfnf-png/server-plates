require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const db = require('./db');

// ── TELEGRAM BOT ──
const TelegramBot = require('node-telegram-bot-api');
const BOT_TOKEN = process.env.BOT_TOKEN;
const BOT_USERNAME = process.env.BOT_USERNAME || 'cardrop_game_bot';
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'https://server-plates-production.up.railway.app';

let bot = null;
if (BOT_TOKEN) {
  bot = new TelegramBot(BOT_TOKEN);
  console.log('Telegram бот инициализирован (webhook режим)');
  bot.onText(/\/start(.*)/, async (msg, match) => {
    const chatId = msg.chat.id;
    const param = (match[1] || '').trim();
    if (param.startsWith('duel_')) {
      const roomId = param.replace('duel_', '');
      let roomExists = false;
      try {
        const { rows } = await db.query("SELECT id, status FROM duel_rooms WHERE id=$1", [roomId]);
        roomExists = rows.length > 0 && rows[0].status === 'waiting';
      } catch (e) {}
      if (!roomExists) return bot.sendMessage(chatId, '❌ Комната не найдена или уже недоступна.', { parse_mode: 'HTML' });
      const webAppUrl = `https://t.me/${BOT_USERNAME}/cardrop?startapp=duel_${roomId}`;
      return bot.sendMessage(chatId,
        `⚔️ <b>Вызов на дуэль!</b>\n\nТебя приглашают сыграть в дуэль номерных знаков.\nКод комнаты: <code>${roomId}</code>`,
        { parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: '⚔️ Принять вызов', web_app: { url: webAppUrl } }]] } }
      );
    }
    const webAppUrl = `https://t.me/${BOT_USERNAME}/cardrop`;
    return bot.sendMessage(chatId,
      '🚗 <b>CarDrop</b> — генератор номерных знаков!\n\nКрути номера, собирай коллекцию, сражайся в дуэлях.',
      { parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{ text: '🎮 Играть', web_app: { url: webAppUrl } }]] } }
    );
  });
} else {
  console.warn('BOT_TOKEN не задан — Telegram бот не запущен');
}

// ── ИНИЦИАЛИЗАЦИЯ БД ──
async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS duel_rooms (
      id VARCHAR(10) PRIMARY KEY,
      flag VARCHAR(5) DEFAULT 'RU',
      goal VARCHAR(30) DEFAULT 'same_digits',
      combo_letters VARCHAR(10) DEFAULT '',
      stake INTEGER DEFAULT 0,
      status VARCHAR(20) DEFAULT 'waiting',
      player1_id VARCHAR(50),
      player1_username VARCHAR(100) DEFAULT '',
      player2_id VARCHAR(50),
      player2_username VARCHAR(100) DEFAULT '',
      winner VARCHAR(50),
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS players (
      telegram_id VARCHAR(50) PRIMARY KEY,
      username VARCHAR(100),
      coins INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
  // Глобальная таблица номеров
  await db.query(`
    CREATE TABLE IF NOT EXISTS plates (
      id SERIAL PRIMARY KEY,
      plate_key VARCHAR(40) UNIQUE NOT NULL,
      country VARCHAR(5) NOT NULL,
      region VARCHAR(10) NOT NULL,
      chars VARCHAR(20) NOT NULL,
      upgrades TEXT DEFAULT '',
      owner_id VARCHAR(50) NOT NULL,
      acquired_at TIMESTAMP DEFAULT NOW(),
      listed_at TIMESTAMP,
      listed_price INTEGER,
      status VARCHAR(20) DEFAULT 'owned',
      seller_name VARCHAR(100) DEFAULT ''
    );
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_plates_owner ON plates(owner_id);`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_plates_status ON plates(status);`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_plates_key ON plates(plate_key);`);
  // Таблица логов действий игрока
  await db.query(`
    CREATE TABLE IF NOT EXISTS player_logs (
      id SERIAL PRIMARY KEY,
      telegram_id VARCHAR(50) NOT NULL,
      action VARCHAR(50) NOT NULL,
      details JSONB DEFAULT '{}',
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_logs_player ON player_logs(telegram_id);`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_logs_time ON player_logs(created_at DESC);`);
  // Добавляем новые колонки если их нет (миграция)
  await db.query(`ALTER TABLE plates ADD COLUMN IF NOT EXISTS seller_name VARCHAR(100) DEFAULT ''`).catch(()=>{});
  await db.query(`ALTER TABLE duel_rooms ADD COLUMN IF NOT EXISTS player1_username VARCHAR(100) DEFAULT ''`).catch(()=>{});
  await db.query(`ALTER TABLE duel_rooms ADD COLUMN IF NOT EXISTS player2_username VARCHAR(100) DEFAULT ''`).catch(()=>{});
  console.log('БД готова');
}
initDB();

const app = express();
const httpServer = http.createServer(app);
const io = new Server(httpServer, { cors: { origin: '*', methods: ['GET', 'POST'] } });
app.use(cors());
app.use(express.json());

app.get('/', (req, res) => res.json({ status: 'ok', message: 'Plates Server работает!' }));

// ── WEBHOOK ──
app.post('/webhook', (req, res) => { if (bot) bot.processUpdate(req.body); res.sendStatus(200); });
app.get('/setwebhook', async (req, res) => {
  if (!bot || !WEBHOOK_URL) return res.json({ ok: false, error: 'BOT_TOKEN или WEBHOOK_URL не заданы' });
  try {
    const webhookUrl = `${WEBHOOK_URL}/webhook`;
    const result = await bot.setWebHook(webhookUrl);
    res.json({ ok: true, webhook_url: webhookUrl, result });
  } catch (e) { res.json({ ok: false, error: e.message }); }
});
app.get('/webhookinfo', async (req, res) => {
  if (!bot) return res.json({ ok: false, error: 'Бот не запущен' });
  try { res.json({ ok: true, info: await bot.getWebHookInfo() }); }
  catch (e) { res.json({ ok: false, error: e.message }); }
});

// ══════════════════════════════════════════════════════════
//  НОМЕРНЫЕ ЗНАКИ — УНИКАЛЬНОСТЬ
// ══════════════════════════════════════════════════════════

// Ключ номера: "RU:А777АА:77"  (country:chars:region)
function makePlateKey(country, region, chars) {
  return `${country}:${chars.toUpperCase()}:${region.toUpperCase()}`;
}

// POST /plate/claim — зарегистрировать выбитый номер
// Body: { telegram_id, country, region, chars, upgrades }
// Ответ: { ok:true, plate_key } или { ok:false, taken:true }
app.post('/plate/claim', async (req, res) => {
  try {
    const { telegram_id, country, region, chars, upgrades } = req.body;
    if (!telegram_id || !country || !region || !chars)
      return res.status(400).json({ ok: false, error: 'Не хватает полей' });

    const plate_key = makePlateKey(country, region, chars);

    // Проверяем текущий статус номера
    const { rows: existing } = await db.query(
      'SELECT id, owner_id, status FROM plates WHERE plate_key=$1', [plate_key]
    );

    if (existing.length > 0) {
      const ex = existing[0];
      // Номер наш — обновляем апгрейды и возвращаем успех
      if (ex.owner_id === telegram_id) {
        await db.query(
          'UPDATE plates SET upgrades=$1 WHERE plate_key=$2 AND owner_id=$3',
          [upgrades || '', plate_key, telegram_id]
        );
        return res.json({ ok: true, plate_key, id: ex.id });
      }
      // Номер чужой — занят
      return res.json({ ok: false, taken: true, plate_key });
    }

    // Свободный номер — занимаем
    const result = await db.query(
      `INSERT INTO plates (plate_key, country, region, chars, upgrades, owner_id)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (plate_key) DO NOTHING
       RETURNING id`,
      [plate_key, country, region, chars.toUpperCase(), upgrades || '', telegram_id]
    );
    if (result.rows.length > 0) {
      await db.query(
        `INSERT INTO player_logs (telegram_id, action, details) VALUES ($1,'plate_claim',$2)`,
        [telegram_id, JSON.stringify({ plate_key, country, region, chars })]
      ).catch(()=>{});
    }

    if (result.rows.length === 0) {
      // Кто-то успел занять между проверкой и вставкой — проверяем чей
      const { rows: raceRows } = await db.query(
        'SELECT id, owner_id FROM plates WHERE plate_key=$1', [plate_key]
      );
      if (raceRows.length && raceRows[0].owner_id === telegram_id) {
        return res.json({ ok: true, plate_key, id: raceRows[0].id });
      }
      return res.json({ ok: false, taken: true, plate_key });
    }

    res.json({ ok: true, plate_key, id: result.rows[0].id });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /plate/check — проверить свободен ли номер
app.post('/plate/check', async (req, res) => {
  try {
    const { country, region, chars } = req.body;
    const plate_key = makePlateKey(country, region, chars);
    const { rows } = await db.query('SELECT owner_id, status FROM plates WHERE plate_key=$1', [plate_key]);
    if (!rows.length) return res.json({ ok: true, free: true, plate_key });
    res.json({ ok: true, free: false, plate_key, owner_id: rows[0].owner_id, status: rows[0].status });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /plate/release — освободить номер (удаление из инвентаря)
app.post('/plate/release', async (req, res) => {
  try {
    const { telegram_id, plate_key } = req.body;
    const result = await db.query(
      "DELETE FROM plates WHERE plate_key=$1 AND owner_id=$2 AND status='owned' RETURNING id",
      [plate_key, telegram_id]
    );
    if (!result.rows.length) return res.status(403).json({ ok: false, error: 'Номер не найден или не ваш' });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// GET /plates/player/:telegram_id — все номера игрока
app.get('/plates/player/:telegram_id', async (req, res) => {
  try {
    const { rows } = await db.query(
      'SELECT * FROM plates WHERE owner_id=$1 ORDER BY acquired_at DESC',
      [req.params.telegram_id]
    );
    res.json({ ok: true, plates: rows });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /plate/upgrade — добавить улучшение к номеру
app.post('/plate/upgrade', async (req, res) => {
  try {
    const { telegram_id, plate_key, upgrade_key } = req.body;
    const { rows } = await db.query(
      'SELECT upgrades FROM plates WHERE plate_key=$1 AND owner_id=$2',
      [plate_key, telegram_id]
    );
    if (!rows.length) return res.status(403).json({ ok: false, error: 'Номер не найден' });
    const current = rows[0].upgrades ? rows[0].upgrades.split(',').filter(Boolean) : [];
    if (!current.includes(upgrade_key)) current.push(upgrade_key);
    await db.query('UPDATE plates SET upgrades=$1 WHERE plate_key=$2 AND owner_id=$3', [current.join(','), plate_key, telegram_id]);
    res.json({ ok: true, upgrades: current });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ══════════════════════════════════════════════════════════
//  МАРКЕТПЛЕЙС
// ══════════════════════════════════════════════════════════

// POST /market/list — выставить номер на продажу
app.post('/market/list', async (req, res) => {
  try {
    const { telegram_id, plate_key, price, seller_name } = req.body;
    if (!plate_key) return res.status(400).json({ ok: false, error: 'Не указан plate_key' });
    if (!price || price < 1) return res.status(400).json({ ok: false, error: 'Укажи цену' });

    // Проверяем что номер принадлежит продавцу и не занят/не продаётся
    const { rows: cur } = await db.query(
      'SELECT status, owner_id FROM plates WHERE plate_key=$1', [plate_key]
    );
    if (!cur.length) return res.status(404).json({ ok: false, error: 'Номер не найден' });
    if (cur[0].owner_id !== telegram_id) return res.status(403).json({ ok: false, error: 'Это не ваш номер' });
    if (cur[0].status === 'listed') {
      // Уже выставлен — просто обновляем цену (идемпотентно)
      const result = await db.query(
        `UPDATE plates SET listed_price=$1, seller_name=COALESCE($4,'Игрок'), listed_at=NOW()
         WHERE plate_key=$2 AND owner_id=$3 RETURNING *`,
        [price, plate_key, telegram_id, seller_name || 'Игрок']
      );
      io.emit('market_updated');
      return res.json({ ok: true, plate: result.rows[0] });
    }

    const result = await db.query(
      `UPDATE plates SET status='listed', listed_at=NOW(), listed_price=$1, seller_name=COALESCE($4,'Игрок')
       WHERE plate_key=$2 AND owner_id=$3 AND status='owned' RETURNING *`,
      [price, plate_key, telegram_id, seller_name || 'Игрок']
    );
    if (!result.rows.length) return res.status(403).json({ ok: false, error: 'Номер не найден или уже выставлен' });
    io.emit('market_updated');
    res.json({ ok: true, plate: result.rows[0] });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /market/unlist — снять с продажи
app.post('/market/unlist', async (req, res) => {
  try {
    const { telegram_id, plate_key } = req.body;
    const result = await db.query(
      `UPDATE plates SET status='owned', listed_at=NULL, listed_price=NULL
       WHERE plate_key=$1 AND owner_id=$2 AND status='listed' RETURNING id`,
      [plate_key, telegram_id]
    );
    if (!result.rows.length) return res.status(403).json({ ok: false, error: 'Номер не найден' });
    io.emit('market_updated');
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// GET /market/listings — все активные лоты с фильтрами и сортировкой
// Query: ?country=RU&region=77&upgrade=glow&sort=newest|cheapest|expensive&limit=20&offset=0
app.get('/market/listings', async (req, res) => {
  try {
    const { country, region, upgrade, sort = 'newest', limit = 20, offset = 0 } = req.query;
    let where = ["status='listed'"];
    const params = [];
    let i = 1;
    if (country) { where.push(`country=$${i++}`); params.push(country); }
    if (region)  { where.push(`region=$${i++}`);  params.push(region); }
    if (upgrade) { where.push(`upgrades LIKE $${i++}`); params.push(`%${upgrade}%`); }
    const orderMap = { newest: 'listed_at DESC', cheapest: 'listed_price ASC', expensive: 'listed_price DESC' };
    const order = orderMap[sort] || 'listed_at DESC';
    params.push(parseInt(limit));
    params.push(parseInt(offset));
    const { rows } = await db.query(
      `SELECT p.*, COALESCE(p.seller_name, pl.username, 'Игрок') as seller_name FROM plates p
       LEFT JOIN players pl ON pl.telegram_id = p.owner_id
       WHERE ${where.join(' AND ')} ORDER BY ${order}
       LIMIT $${i++} OFFSET $${i++}`,
      params
    );
    const { rows: countRows } = await db.query(
      `SELECT COUNT(*) FROM plates WHERE ${where.join(' AND ')}`,
      params.slice(0, params.length - 2)
    );
    res.json({ ok: true, listings: rows, total: parseInt(countRows[0].count) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /market/buy — купить номер (атомарно, защита от двойного владения)
app.post('/market/buy', async (req, res) => {
  const client = await db.connect();
  try {
    const { buyer_id, plate_key } = req.body;
    if (!buyer_id || !plate_key) {
      client.release();
      return res.status(400).json({ ok: false, error: 'Не хватает полей' });
    }
    await client.query('BEGIN');

    // Блокируем строку номера — никто другой не купит одновременно
    const { rows } = await client.query(
      "SELECT * FROM plates WHERE plate_key=$1 AND status='listed' FOR UPDATE NOWAIT", [plate_key]
    );
    if (!rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'Лот не найден или уже продан' });
    }
    const plate = rows[0];
    if (plate.owner_id === buyer_id) {
      await client.query('ROLLBACK');
      return res.status(400).json({ ok: false, error: 'Нельзя купить свой номер' });
    }

    // Блокируем строку покупателя
    const { rows: buyerRows } = await client.query(
      'SELECT coins FROM players WHERE telegram_id=$1 FOR UPDATE', [buyer_id]
    );
    if (!buyerRows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'Покупатель не найден' });
    }
    if (buyerRows[0].coins < plate.listed_price) {
      await client.query('ROLLBACK');
      return res.status(400).json({ ok: false, error: 'Недостаточно монет' });
    }

    // Всё в одной транзакции: списать монеты, начислить продавцу, сменить владельца
    const sellerAmount = plate.listed_price;
    await client.query(
      'UPDATE players SET coins=coins-$1, updated_at=NOW() WHERE telegram_id=$2',
      [plate.listed_price, buyer_id]
    );
    // Создаём запись продавца если её вдруг нет (INSERT ... ON CONFLICT) — начисляем безопасно
    await client.query(
      `INSERT INTO players (telegram_id, username, coins) VALUES ($1,'Игрок',$2)
       ON CONFLICT (telegram_id) DO UPDATE SET coins=players.coins+$2, updated_at=NOW()`,
      [plate.owner_id, sellerAmount]
    );
    await client.query(
      "UPDATE plates SET owner_id=$1, status='owned', listed_at=NULL, listed_price=NULL, acquired_at=NOW() WHERE plate_key=$2",
      [buyer_id, plate_key]
    );
    await client.query('COMMIT');

    // Эмитим событие ПОСЛЕ успешного коммита
    io.emit('plate_sold', {
      plate_key,
      buyer_id,
      seller_id: plate.owner_id,
      price: plate.listed_price,
      seller_amount: sellerAmount,
      plate_data: {
        country: plate.country,
        region: plate.region,
        chars: plate.chars,
        upgrades: plate.upgrades || ''
      }
    });
    io.emit('market_updated');

    res.json({
      ok: true,
      price: plate.listed_price,
      seller_amount: sellerAmount,
      plate_data: { country: plate.country, region: plate.region, chars: plate.chars, upgrades: plate.upgrades || '' }
    });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    // Если строка заблокирована другой транзакцией — возвращаем понятную ошибку
    if (e.code === '55P03') {
      return res.status(409).json({ ok: false, error: 'Лот уже покупается другим игроком, попробуйте снова' });
    }
    res.status(500).json({ ok: false, error: e.message });
  } finally { client.release(); }
});

// ── ДУЭЛИ ──
app.post('/duel/create', async (req, res) => {
  try {
    const { flag, goal, combo_letters, stake, player1_id, player1_username } = req.body;
    const id = Math.random().toString(36).slice(2, 7).toUpperCase();
    await db.query(
      `INSERT INTO duel_rooms (id,flag,goal,combo_letters,stake,player1_id,player1_username) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [id, flag, goal, combo_letters || '', stake || 0, player1_id, player1_username || '']);
    res.json({ ok: true, room_id: id });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/duel/room/:id', async (req, res) => {
  try {
    const { rows } = await db.query('SELECT * FROM duel_rooms WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: 'Не найдена' });
    res.json({ ok: true, room: rows[0] });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/duel/join/:id', async (req, res) => {
  try {
    const { player2_id, player2_username } = req.body;
    const { rows } = await db.query('SELECT * FROM duel_rooms WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: 'Не найдена' });
    const room = rows[0];
    if (room.status !== 'waiting') return res.status(400).json({ ok: false, error: 'Недоступна' });
    if (room.player2_id) return res.status(400).json({ ok: false, error: 'Место занято' });
    await db.query(
      'UPDATE duel_rooms SET player2_id=$1, player2_username=$2, updated_at=NOW() WHERE id=$3',
      [player2_id, player2_username || '', req.params.id]);
    // Передаём имя p2 в событии — p1 увидит никнейм вошедшего
    io.to(req.params.id).emit('player_joined', {
      player2_id,
      username: player2_username || '',
      name: player2_username || ''
    });
    // Также возвращаем имя p1 для p2
    res.json({ ok: true, player1_username: room.player1_username || '' });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/duel/start/:id', async (req, res) => {
  try {
    await db.query("UPDATE duel_rooms SET status='battle', updated_at=NOW() WHERE id=$1", [req.params.id]);
    io.to(req.params.id).emit('battle_started');
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/duel/finish/:id', async (req, res) => {
  try {
    const { winner } = req.body;
    const { rows: room } = await db.query('SELECT * FROM duel_rooms WHERE id=$1', [req.params.id]);
    await db.query("UPDATE duel_rooms SET status='finished', winner=$1, updated_at=NOW() WHERE id=$2", [winner, req.params.id]);
    io.to(req.params.id).emit('battle_finished', { winner });
    // Логируем дуэль для обоих игроков
    if (room[0]) {
      const r = room[0];
      const loser = winner === r.player1_id ? r.player2_id : r.player1_id;
      const loser_name = winner === r.player1_id ? r.player2_username : r.player1_username;
      const winner_name = winner === r.player1_id ? r.player1_username : r.player2_username;
      await db.query(
        `INSERT INTO player_logs (telegram_id, action, details) VALUES ($1,'duel_win',$2)`,
        [winner, JSON.stringify({ room_id: r.id, opponent_id: loser, opponent_name: loser_name, stake: r.stake, flag: r.flag, goal: r.goal })]
      ).catch(()=>{});
      if (loser) await db.query(
        `INSERT INTO player_logs (telegram_id, action, details) VALUES ($1,'duel_loss',$2)`,
        [loser, JSON.stringify({ room_id: r.id, opponent_id: winner, opponent_name: winner_name, stake: r.stake, flag: r.flag, goal: r.goal })]
      ).catch(()=>{});
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /duel/cancel/:id — лидер отменяет комнату (нет соперника)
app.post('/duel/cancel/:id', async (req, res) => {
  try {
    const { player_id } = req.body;
    const { rows } = await db.query('SELECT * FROM duel_rooms WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.json({ ok: true }); // уже нет
    const room = rows[0];
    if (room.player1_id !== player_id) return res.status(403).json({ ok: false, error: 'Не лидер' });
    // Удаляем комнату полностью
    await db.query('DELETE FROM duel_rooms WHERE id=$1', [req.params.id]);
    // Уведомляем всех в комнате
    io.to(req.params.id).emit('room_cancelled');
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /duel/leave/:id — p2 выходит из комнаты ожидания
app.post('/duel/leave/:id', async (req, res) => {
  try {
    const { player_id } = req.body;
    const { rows } = await db.query('SELECT * FROM duel_rooms WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.json({ ok: true });
    const room = rows[0];
    if (room.player2_id !== player_id) return res.json({ ok: true });
    // Убираем p2 из комнаты
    await db.query(
      "UPDATE duel_rooms SET player2_id=NULL, player2_username='', updated_at=NOW() WHERE id=$1",
      [req.params.id]);
    // Уведомляем p1 что соперник ушёл
    io.to(req.params.id).emit('player_left', { player_id });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /duel/transfer/:id — передача лидерства от p1 к p2
app.post('/duel/transfer/:id', async (req, res) => {
  try {
    const { player_id } = req.body; // player_id = текущий лидер (p1)
    const { rows } = await db.query('SELECT * FROM duel_rooms WHERE id=$1', [req.params.id]);
    if (!rows.length) return res.json({ ok: true });
    const room = rows[0];
    if (room.player1_id !== player_id) return res.status(403).json({ ok: false, error: 'Не лидер' });
    if (!room.player2_id) return res.status(400).json({ ok: false, error: 'Нет соперника' });
    // p2 становится p1, слот p2 очищается
    await db.query(
      `UPDATE duel_rooms SET
        player1_id=$1, player1_username=$2,
        player2_id=NULL, player2_username='',
        updated_at=NOW()
       WHERE id=$3`,
      [room.player2_id, room.player2_username || '', req.params.id]);
    // Уведомляем всех: новый лидер и его имя
    io.to(req.params.id).emit('leader_transferred', {
      new_leader_id: room.player2_id,
      new_leader_name: room.player2_username || ''
    });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── ИГРОКИ ──
app.post('/player/sync', async (req, res) => {
  try {
    const { telegram_id, username, coins } = req.body;
    // Первый вход — создаём игрока. Повторный — обновляем только username, coins НЕ трогаем
    await db.query(
      `INSERT INTO players (telegram_id, username, coins) VALUES ($1,$2,$3)
       ON CONFLICT (telegram_id) DO UPDATE SET username=EXCLUDED.username, updated_at=NOW()`,
      [telegram_id, username || 'Игрок', coins || 0]
    );
    // Всегда возвращаем актуальный серверный баланс
    const { rows } = await db.query('SELECT * FROM players WHERE telegram_id=$1', [telegram_id]);
    // Логируем вход
    await db.query(
      `INSERT INTO player_logs (telegram_id, action, details) VALUES ($1,'login',$2)`,
      [telegram_id, JSON.stringify({ username })]
    ).catch(()=>{});
    res.json({ ok: true, player: rows[0] });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/player/balance', async (req, res) => {
  try {
    const { telegram_id, coins } = req.body;
    // Получаем старый баланс для лога
    const { rows: old } = await db.query('SELECT coins FROM players WHERE telegram_id=$1', [telegram_id]);
    await db.query('UPDATE players SET coins=$1, updated_at=NOW() WHERE telegram_id=$2', [coins, telegram_id]);
    const diff = coins - (old[0]?.coins || 0);
    if (diff !== 0) {
      await db.query(
        `INSERT INTO player_logs (telegram_id, action, details) VALUES ($1,'balance',$2)`,
        [telegram_id, JSON.stringify({ old_coins: old[0]?.coins, new_coins: coins, diff })]
      ).catch(()=>{});
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// GET /player/coins/:telegram_id — получить баланс игрока
app.get('/player/coins/:telegram_id', async (req, res) => {
  try {
    const { rows } = await db.query('SELECT coins FROM players WHERE telegram_id=$1', [req.params.telegram_id]);
    if (!rows.length) return res.status(404).json({ ok: false, error: 'Игрок не найден' });
    res.json({ ok: true, coins: rows[0].coins });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// GET /player/inventory-count/:telegram_id — количество номеров в инвентаре
app.get('/player/inventory-count/:telegram_id', async (req, res) => {
  try {
    const { rows } = await db.query(
      "SELECT COUNT(*) FROM plates WHERE owner_id=$1 AND status IN ('owned','listed')",
      [req.params.telegram_id]
    );
    res.json({ ok: true, count: parseInt(rows[0].count) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ══════════════════════════════════════════════════════════
//  ИГРОВАЯ СТАТИСТИКА
// ══════════════════════════════════════════════════════════

app.get('/stats/game', async (req, res) => {
  try {
    const { rows: totals } = await db.query(`
      SELECT
        (SELECT COUNT(*) FROM players) AS total_players,
        (SELECT COALESCE(SUM(coins),0) FROM players) AS total_coins,
        (SELECT COUNT(*) FROM plates WHERE status IN ('owned','listed')) AS total_plates,
        (SELECT COUNT(*) FROM plates WHERE status='listed') AS plates_on_market,
        (SELECT COUNT(*) FROM duel_rooms WHERE status='finished') AS duels_finished
    `);
    const { rows: days } = await db.query(`
      SELECT
        TO_CHAR(created_at, 'DD.MM') AS date,
        TO_CHAR(created_at, 'YYYY-MM-DD') AS iso_date,
        COUNT(*) AS new_players
      FROM players
      WHERE created_at >= NOW() - INTERVAL '30 days'
      GROUP BY TO_CHAR(created_at, 'DD.MM'), TO_CHAR(created_at, 'YYYY-MM-DD')
      ORDER BY TO_CHAR(created_at, 'YYYY-MM-DD') ASC
    `);
    res.json({ ok: true, ...totals[0], days });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.get('/stats/players', async (req, res) => {
  try {
    const { rows } = await db.query(`
      SELECT
        p.telegram_id,
        p.username,
        p.coins,
        p.created_at,
        COUNT(CASE WHEN pl.status IN ('owned','listed') THEN 1 END) AS plates_count
      FROM players p
      LEFT JOIN plates pl ON pl.owner_id = p.telegram_id
      GROUP BY p.telegram_id, p.username, p.coins, p.created_at
      ORDER BY p.coins DESC
    `);
    res.json({ ok: true, players: rows, total: rows.length });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ══════════════════════════════════════════════════════════
//  ПРОФИЛЬ ИГРОКА (для админки)
// ══════════════════════════════════════════════════════════
app.get('/stats/player/:telegram_id', async (req, res) => {
  const tid = req.params.telegram_id;
  try {
    // Основные данные
    const { rows: playerRows } = await db.query('SELECT * FROM players WHERE telegram_id=$1', [tid]);
    if (!playerRows.length) return res.status(404).json({ ok: false, error: 'Игрок не найден' });
    const player = playerRows[0];

    // Инвентарь (все номера)
    const { rows: plates } = await db.query(
      `SELECT plate_key, country, region, chars, upgrades, status, acquired_at, listed_price
       FROM plates WHERE owner_id=$1 AND status IN ('owned','listed') ORDER BY acquired_at DESC`,
      [tid]
    );

    // Дуэли
    const { rows: duels } = await db.query(
      `SELECT id, flag, goal, stake, player1_id, player1_username, player2_id, player2_username,
              winner, status, created_at
       FROM duel_rooms
       WHERE (player1_id=$1 OR player2_id=$1) AND status='finished'
       ORDER BY created_at DESC LIMIT 30`,
      [tid]
    );
    const duels_won  = duels.filter(d => d.winner === tid).length;
    const duels_lost = duels.filter(d => d.winner && d.winner !== tid).length;

    // Логи действий
    const { rows: logs } = await db.query(
      `SELECT action, details, created_at FROM player_logs
       WHERE telegram_id=$1 ORDER BY created_at DESC LIMIT 50`,
      [tid]
    );

    res.json({ ok: true, player, plates, duels, duels_won, duels_lost, logs });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── SOCKET.IO ──
io.on('connection', (socket) => {
  socket.on('join_room', (roomId) => socket.join(roomId));
  socket.on('leave_room', (roomId) => socket.leave(roomId));
  socket.on('surrender', ({ roomId }) => socket.to(roomId).emit('opponent_surrendered'));
  socket.on('plate_spun', ({ roomId, plateObj }) => socket.to(roomId).emit('opp_plate_spun', { plateObj }));
  socket.on('player_won', ({ roomId }) => socket.to(roomId).emit('opponent_won'));
  // Пробросы событий выхода и передачи лидерства
  socket.on('cancel_room', ({ roomId }) => socket.to(roomId).emit('room_cancelled'));
  socket.on('player_left', ({ roomId }) => socket.to(roomId).emit('player_left', {}));
  socket.on('leader_left', ({ roomId, newLeaderName }) =>
    socket.to(roomId).emit('leader_transferred', { new_leader_name: newLeaderName })
  );
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => console.log(`Сервер запущен на порту ${PORT}`));
