import express from 'express';
import cors from 'cors';
import { initializeApp } from 'firebase/app';
import { getDatabase, ref, query, orderByKey, limitToFirst, startAfter, get, onChildAdded } from 'firebase/database';
import fs from 'fs';

const app = express();

// Configurar CORS
app.use(cors({
  origin: [
    'http://localhost:5500',
    'http://127.0.0.1:5500',
    'http://localhost:3003',
    'https://tu-frontend-url.com'
  ],
  credentials: false,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json());
app.use(express.static('.'));

// Configuración Firebase para la segunda base de datos
const firebaseConfig = {
  apiKey: "AIzaSyDP22R5HH9m1pQCLuG9qk86pU1XDGlLdSo",
  authDomain: "base1743.firebaseapp.com",
  databaseURL: "https://base1743-default-rtdb.firebaseio.com",
  projectId: "base1743",
  storageBucket: "base1743.firebasestorage.app",
  messagingSenderId: "834684474812",
  appId: "1:834684474812:web:c334aa695bf49fa11007b9",
  measurementId: "G-SFL0XGECQJ"
};

// Inicializar Firebase
const firebaseApp = initializeApp(firebaseConfig);
const db = getDatabase(firebaseApp);

// Archivos de cache para la segunda base de datos
const CACHE_FILE = 'sensores-cache.jsonl';
const LAST_KEY_FILE = 'sensores-last-key.txt';

// Set para evitar duplicados
const processedIDs = new Set();

// Función para guardar la última key procesada
function saveLastKey(key) {
  try {
    fs.writeFileSync(LAST_KEY_FILE, key, 'utf8');
    console.log(`💾 Última key guardada: ${key}`);
  } catch (error) {
    console.error('Error guardando última key:', error);
  }
}

// Función para cargar la última key procesada
function loadLastKey() {
  try {
    if (fs.existsSync(LAST_KEY_FILE)) {
      const key = fs.readFileSync(LAST_KEY_FILE, 'utf8').trim();
      console.log(`📂 Última key cargada: ${key}`);
      return key;
    }
  } catch (error) {
    console.warn('Error cargando última key:', error);
  }
  return null;
}

// Función para guardar registro en JSONL
function appendRecord(record) {
  if (!record || !record.id || processedIDs.has(record.id)) return;
  
  try {
    const jsonLine = JSON.stringify(record) + "\n";
    fs.appendFileSync(CACHE_FILE, jsonLine, "utf8");
    processedIDs.add(record.id);
    console.log(`➕ Registro guardado: ${record.id}`);
  } catch (error) {
    console.error('Error guardando registro:', error, record);
  }
}

// Función para cargar IDs recientes del cache
function loadRecentIDs(linesToRead = 2000) {
  try {
    if (!fs.existsSync(CACHE_FILE)) return;
    const stats = fs.statSync(CACHE_FILE);
    const size = stats.size;
    const fd = fs.openSync(CACHE_FILE, "r");
    const bufferSize = Math.min(200 * 1024, size);
    const buffer = Buffer.alloc(bufferSize);
    const position = Math.max(0, size - bufferSize);
    fs.readSync(fd, buffer, 0, bufferSize, position);
    fs.closeSync(fd);

    const lines = buffer.toString().split("\n").slice(-linesToRead);
    let loaded = 0;
    lines.forEach(line => {
      if (!line.trim() || loaded >= linesToRead) return;
      try {
        const obj = JSON.parse(line);
        if (obj && obj.id) {
          processedIDs.add(obj.id);
          loaded++;
        }
      } catch (error) {
        // Ignorar líneas corruptas
      }
    });

    console.log(`📂 Cargados ${processedIDs.size} IDs recientes`);
  } catch (err) {
    console.warn("⚠️ Error cargando cache:", err.message);
  }
}

// Función para descargar datos en lotes
async function downloadInBatches(path, batchSize = 300) {
  let lastKey = loadLastKey();
  let finished = false;
  let totalDownloaded = 0;
  const maxBatches = 20;
  let batchCount = 0;
  
  console.log("⏳ Descarga inicial optimizada...");

  while (!finished && batchCount < maxBatches) {
    try {
      const q = lastKey
        ? query(ref(db, path), orderByKey(), startAfter(lastKey), limitToFirst(batchSize))
        : query(ref(db, path), orderByKey(), limitToFirst(batchSize));

      const snap = await get(q);
      if (!snap.exists()) break;

      const data = snap.val();
      const keys = Object.keys(data);

      if (keys.length === 0) break;

      for (const key of keys) {
        const item = data[key];
        if (item && item.fechaHora && item.sensores) {
          // Solo procesar datos originales con fechaHora
          appendRecord({
            id: key,
            ...item
          });
          lastKey = key;
          totalDownloaded++;
        }
      }

      if (lastKey) saveLastKey(lastKey);
      batchCount++;
      
      if (batchCount % 5 === 0) {
        await new Promise(resolve => setTimeout(resolve, 100));
        if (global.gc) global.gc();
      }
      
      if (keys.length < batchSize) finished = true;
      
    } catch (error) {
      console.error('Error en lote:', error.message);
      break;
    }
  }

  console.log(`📥 Descarga completa: ${totalDownloaded} registros`);
}

// Función para escuchar nuevos datos
function listenForNew(path) {
  const lastKey = loadLastKey();
  const q = lastKey 
    ? query(ref(db, path), orderByKey(), startAfter(lastKey))
    : ref(db, path);

  onChildAdded(q, (snapshot) => {
    const key = snapshot.key;
    const data = snapshot.val();
    
    // Solo procesar datos con fechaHora (estructura original)
    if (data && !processedIDs.has(key) && data.fechaHora && data.sensores) {
      appendRecord({
        id: key,
        ...data
      });
      saveLastKey(key);
    }
  });

  console.log("👂 Escuchando nuevos datos en tiempo real...");
}

// API para obtener información general
app.get('/api/sensores-info', (req, res) => {
  try {
    if (!fs.existsSync(CACHE_FILE)) {
      return res.json({ total: 0, message: 'No hay datos disponibles' });
    }

    // Leer solo las últimas líneas para obtener el registro más reciente
    const stats = fs.statSync(CACHE_FILE);
    const size = stats.size;
    const fd = fs.openSync(CACHE_FILE, 'r');
    const bufferSize = Math.min(10 * 1024, size);
    const buffer = Buffer.alloc(bufferSize);
    const position = Math.max(0, size - bufferSize);
    fs.readSync(fd, buffer, 0, bufferSize, position);
    fs.closeSync(fd);

    const lines = buffer.toString().split('\n').filter(line => line.trim());
    
    if (lines.length === 0) {
      return res.json({ total: 0, message: 'Cache vacío' });
    }

    // Obtener el último registro válido
    let lastRecord = null;
    for (let i = lines.length - 1; i >= 0; i--) {
      try {
        lastRecord = JSON.parse(lines[i]);
        break;
      } catch (e) {
        continue;
      }
    }
    
    if (!lastRecord) {
      return res.json({ total: 0, message: 'No se pudo leer el último registro' });
    }
    
    const info = {
      total: lines.length,
      lastDate: lastRecord.fechaHora || null,
      lastRecord: lastRecord,
      message: `Último registro: ${lastRecord.fechaHora || 'Sin fecha'}`
    };
    
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.json(info);
  } catch (error) {
    console.error('Error en /api/sensores-info:', error);
    res.json({ error: error.message, total: 0 });
  }
});

// API básica para verificar que el servidor funciona
app.get('/', (req, res) => {
  res.json({ message: 'Servidor de sensores funcionando', status: 'OK' });
});

// API para obtener datos paginados
app.get('/api/sensores-data', (req, res) => {
  const { limit = 1000, offset = 0 } = req.query;
  const limitNum = Math.min(parseInt(limit), 2000);
  const offsetNum = parseInt(offset);
  
  try {
    if (!fs.existsSync(CACHE_FILE)) {
      return res.json([]);
    }
    
    const content = fs.readFileSync(CACHE_FILE, 'utf8');
    const lines = content.trim().split('\n').filter(line => line.trim());
    
    const requestedLines = lines.slice(offsetNum, offsetNum + limitNum);
    
    const records = requestedLines.map(line => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    }).filter(Boolean);
    
    res.setHeader('Cache-Control', 'public, max-age=30');
    res.json(records);
    
  } catch (error) {
    console.error('Error en API:', error.message);
    res.status(500).json([]);
  }
});

// Inicialización del servidor
async function initServer() {
  console.log("🚀 Iniciando servidor para base de datos Sensores...");
  
  loadRecentIDs(2000);
  
  const PATH = "Sensores";
  
  try {
    await downloadInBatches(PATH, 300);
    console.log('✅ Descarga inicial completada');
  } catch (error) {
    console.error('❌ Error en descarga inicial:', error);
  }

  listenForNew(PATH);
  
  setInterval(() => {
    if (global.gc) {
      global.gc();
      console.log('🧹 Memoria limpiada');
    }
    if (processedIDs.size > 5000) {
      const idsArray = Array.from(processedIDs);
      processedIDs.clear();
      idsArray.slice(-2000).forEach(id => processedIDs.add(id));
      console.log('🧹 IDs optimizados');
    }
  }, 5 * 60 * 1000);
}

// Iniciar servidor
const PORT = process.env.PORT || 3003;
app.listen(PORT, () => {
  console.log(`🌐 Servidor Sensores ejecutándose en puerto ${PORT}`);
  initServer();
});