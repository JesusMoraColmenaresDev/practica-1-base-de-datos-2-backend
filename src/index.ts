import express from 'express';
import cors from 'cors';
import multer from 'multer';
import XLSX from 'xlsx';
import { saveRows } from './prismaService';

const app = express();
app.use(cors());

const upload = multer({ storage: multer.memoryStorage() });

app.get('/', (_req, res) => {
    res.json({ status: 'ok' });
});

app.post('/upload', upload.single('file'), async (req, res) => {
    const file = req.file;
    if (!file) return res.status(400).json({ error: 'No file uploaded (field name: file)' });

    try {
        const workbook = XLSX.read(file.buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];
        const objects = XLSX.utils.sheet_to_json<Record<string, string | number | null>>(sheet, { range: 1, defval: null });
        await saveRows(objects);

        return res.json({ rows: objects, count: objects.length });
    } catch (err) {
        console.error('Failed to parse XLSX:', err);
        return res.status(500).json({ error: 'Failed to parse XLSX' });
    }
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`API listening on http://localhost:${PORT}`));
