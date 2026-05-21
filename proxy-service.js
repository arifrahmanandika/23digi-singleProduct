require("dotenv").config();
const net = require("net");
const mysql = require("mysql2/promise");

const log = (routeName, msg) =>
  console.log(`[${new Date().toLocaleString()}] [${routeName}] => ${msg}`);

// ============================================================
// KONFIGURASI JALUR (ROUTES)
// Tambah atau kurangi objek di array ini sesuai kebutuhan.
// ============================================================
const ROUTES = [
  {
    name: "DIGI-2",
    listenPort: process.env.DIGI2_LISTEN_PORT,
    targetHost: process.env.DIGI2_TARGET_HOST,
    targetPort: process.env.DIGI2_TARGET_PORT,
    delaySendMs: parseInt(process.env.DIGI2_DELAY_MS) || 15000,
  },
  {
    name: "DIGI-3",
    listenPort: process.env.DIGI3_LISTEN_PORT,
    targetHost: process.env.DIGI3_TARGET_HOST,
    targetPort: process.env.DIGI3_TARGET_PORT,
    delaySendMs: parseInt(process.env.DIGI3_DELAY_MS) || 15000,
  },
  {
    name: "DIGI-4",
    listenPort: process.env.DIGI4_LISTEN_PORT,
    targetHost: process.env.DIGI4_TARGET_HOST,
    targetPort: process.env.DIGI4_TARGET_PORT,
    delaySendMs: parseInt(process.env.DIGI4_DELAY_MS) || 15000,
  },
  {
    name: "RITA-2",
    listenPort: process.env.RITA2_LISTEN_PORT,
    targetHost: process.env.RITA2_TARGET_HOST,
    targetPort: process.env.RITA2_TARGET_PORT,
    delaySendMs: parseInt(process.env.RITA2_DELAY_MS) || 15000,
  },
  {
    name: "RITA-3",
    listenPort: process.env.RITA3_LISTEN_PORT,
    targetHost: process.env.RITA3_TARGET_HOST,
    targetPort: process.env.RITA3_TARGET_PORT,
    delaySendMs: parseInt(process.env.RITA3_DELAY_MS) || 15000,
  },
  {
    name: "RITA-4",
    listenPort: process.env.RITA4_LISTEN_PORT,
    targetHost: process.env.RITA4_TARGET_HOST,
    targetPort: process.env.RITA4_TARGET_PORT,
    delaySendMs: parseInt(process.env.RITA4_DELAY_MS) || 15000,
  },
];

// ============================================================
// KONFIGURASI DATABASE (shared untuk semua jalur)
// ============================================================
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT,
});

// ============================================================
// FUNGSI HELPER
// ============================================================

// Ekstrak nomor HP berawalan 628 (total 11-16 digit)
function extractIncomingNumber(dataString) {
  const match = dataString.match(/(628\d{8,13})/);
  return match ? match[0] : null;
}

// Proses data: cari nomor di DB dan replace jika ada transaksi pending
async function processData(routeName, incomingDataStr) {
  try {
    const incomingNumber = extractIncomingNumber(incomingDataStr);

    if (!incomingNumber) {
      return incomingDataStr;
    }

    const coreNumber = incomingNumber.substring(2);
    const searchPattern = `0${coreNumber}.%`;

    const [rows] = await pool.query(
      `SELECT tujuan FROM transaksi 
       WHERE statustransaksi NOT IN ('1','2') 
       AND tujuan LIKE ?`,
      [searchPattern]
    );

    if (rows.length > 0) {
      const dbTujuan = rows[0].tujuan;
      const processedData = incomingDataStr.replace(incomingNumber, dbTujuan);
      log(routeName, `[DB HIT] ${incomingNumber} => ${dbTujuan}`);
      return processedData;
    }

    log(routeName, `[-] Tidak ada transaksi Pending untuk: ${incomingNumber}`);
    return incomingDataStr;
  } catch (error) {
    console.error(`[${routeName}] Error DB:`, error.message);
    return incomingDataStr;
  }
}

// ============================================================
// BUAT TCP SERVER PER JALUR
// ============================================================
function createRouteServer(route) {
  const server = net.createServer((clientSocket) => {
    log(
      route.name,
      `[KONEKSI MASUK] dari ${clientSocket.remoteAddress}:${clientSocket.remotePort}`
    );

    const targetSocket = new net.Socket();

    targetSocket.connect(route.targetPort, route.targetHost, () => {
      log(route.name, `[TERHUBUNG] ke ${route.targetHost}:${route.targetPort}`);
    });

    // Terima data dari sumber
    clientSocket.on("data", async (data) => {
      const dataStr = data.toString();
      log(route.name, `[DATA DITERIMA]`);

      const finalDataStr = await processData(route.name, dataStr);

      log(
        route.name,
        `[PROSES] Menunggu ${route.delaySendMs / 1000} detik sebelum meneruskan...`
      );

      setTimeout(() => {
        if (targetSocket.writable) {
          targetSocket.write(finalDataStr);
          log(route.name, `[DATA DIKIRIM KE ${route.targetHost}:${route.targetPort}]`);
        } else {
          console.error(
            `[${route.name}] [GAGAL] Socket tujuan tidak siap atau tertutup.`
          );
        }
      }, route.delaySendMs);
    });

    // Teruskan data dari target balik ke client (opsional, jika dibutuhkan)
    targetSocket.on("data", (data) => {
      if (clientSocket.writable) {
        clientSocket.write(data);
      }
    });

    clientSocket.on("error", (err) =>
      console.error(`[${route.name}] [CLIENT ERROR]`, err.message)
    );
    targetSocket.on("error", (err) =>
      console.error(`[${route.name}] [TARGET ERROR]`, err.message)
    );

    clientSocket.on("close", () => targetSocket.end());
    targetSocket.on("close", () => clientSocket.end());
  });

  server.listen(route.listenPort, () => {
    log(route.name, `Listening di port ${route.listenPort} => forward ke ${route.targetHost}:${route.targetPort}`);
  });

  server.on("error", (err) =>
    console.error(`[${route.name}] [SERVER ERROR]`, err.message)
  );

  return server;
}

// ============================================================
// JALANKAN SEMUA JALUR
// ============================================================
console.log(`[${new Date().toLocaleString()}] Memulai ${ROUTES.length} jalur proxy...\n`);
ROUTES.forEach((route) => createRouteServer(route));
