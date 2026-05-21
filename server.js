require("dotenv").config();
const net = require("net");
const mysql = require("mysql2/promise");
const log = (msg) => console.log(`[${new Date().toLocaleString()}] => ${msg}`);

const PORT_LISTEN = process.env.PORT_LISTEN || 5701;
const TARGET_HOST = process.env.TARGET_HOST || "127.0.0.1";
const TARGET_PORT = process.env.TARGET_PORT || 5801;
const DELAY_SEND_MS = process.env.DELAY_SEND_MS || 15000; // Delay dalam milidetik (default 15000 ms = 15 detik)

// Konfigurasi Database
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT,
};
const pool = mysql.createPool(dbConfig);

// Fungsi untuk mencari nomor HP berawalan 628... di dalam string data
// Regex ini HANYA menangkap angka 628 diikuti 8-12 digit angka lainnya.
// Teks lain di sekitarnya (huruf besar/kecil) diabaikan.
function extractIncomingNumber(dataString) {
  const match = dataString.match(/(628\d{8,12})/);
  return match ? match[0] : null;
}

// Fungsi untuk Proses data
async function processData(incomingDataStr) {
  try {
    // 1. Ekstrak angka dari data masuk (Contoh dapet: 6282182820370)
    const incomingNumber = extractIncomingNumber(incomingDataStr);

    if (!incomingNumber) {
      return incomingDataStr; // Jika tidak ada angka 628, kembalikan data utuh
    }

    // 2. Ambil angka intinya saja (Contoh jadi: 82182820370)
    const coreNumber = incomingNumber.substring(2);

    // 3. Buat pola pencarian database (Contoh jadi: 082182820370.%)
    const searchPattern = `0${coreNumber}.%`;

    // 4. Cari di database di Transaksi Pending / Selain Sukses / Gagal
    const [rows] = await pool.query(
      `SELECT tujuan FROM transaksi 
             WHERE statustransaksi NOT IN ('1','2') 
             AND tujuan LIKE ?`,
      [searchPattern],
    );

    if (rows.length > 0) {
      // 5. Ambil hasil dari DB (Contoh: 082182820370.6471)
      const dbTujuan = rows[0].tujuan;

      // 6. Replace HANYA nomornya saja di dalam teks asli.
      // Huruf besar/kecil di sisa teks tidak akan terpengaruh sama sekali.
      const processedData = incomingDataStr.replace(incomingNumber, dbTujuan);

      return processedData;
    }

    log(`[-] Tidak ada transaksi Pending untuk nomor: ${incomingNumber}`);
    return incomingDataStr;
  } catch (error) {
    console.error("Error saat memproses data/DB:", error.message);
    return incomingDataStr; // Teruskan saja data utuh jika ada error
  }
}

// --- TCP SERVER & PROXY ---
const server = net.createServer((clientSocket) => {
  log(
    `\n[KONEKSI MASUK] dari ${clientSocket.remoteAddress}:${clientSocket.remotePort}`,
  );

  const targetSocket = new net.Socket();

  targetSocket.connect(TARGET_PORT, TARGET_HOST, () => {
    log(`[TERHUBUNG] ke Terminal tujuan ${TARGET_HOST}:${TARGET_PORT}`);
  });

  // Menerima data dari Port / Terminal SMS
  clientSocket.on("data", async (data) => {
    const dataStr = data.toString();
    log(`[DATA DITERIMA]`);

    // Olah data
    const finalDataStr = await processData(dataStr);

    log(
      `[PROSES] Menunggu ${DELAY_SEND_MS / 1000} detik sebelum meneruskan data...`,
    );

    // Memberikan delay sebelum kirim ke target port / Terminal H2h
    setTimeout(() => {
      if (targetSocket.writable) {
        targetSocket.write(finalDataStr);
        log(`[DATA DIKIRIM KE  ${TARGET_HOST}:${TARGET_PORT}]`);
      } else {
        console.error(
          "[GAGAL] Socket tujuan belum siap atau tertutup saat mencoba mengirim.",
        );
      }
    }, DELAY_SEND_MS);
  });

  clientSocket.on("error", (err) =>
    console.error("[CLIENT ERROR]", err.message),
  );
  targetSocket.on("error", (err) =>
    console.error("[TARGET ERROR]", err.message),
  );

  clientSocket.on("close", () => targetSocket.end());
  targetSocket.on("close", () => clientSocket.end());
});

server.listen(PORT_LISTEN, () => {
  log(`Service berjalan di port ${PORT_LISTEN}...`);
  log(`Meneruskan data ke ${TARGET_HOST}:${TARGET_PORT}`);
});
