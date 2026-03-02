import express from "express";
import cors from "cors";
import multer from "multer";
import XLSX from "xlsx";
import { saveRows } from "./prismaService";

const app = express();
// Allow requests from any origin by reflecting the request origin and enable credentials
app.use(
	cors({
		origin: true,
		methods: ["GET", "HEAD", "PUT", "PATCH", "POST", "DELETE", "OPTIONS"],
		allowedHeaders: [
			"Content-Type",
			"Authorization",
			"Accept",
			"Origin",
			"X-Requested-With",
		],
		credentials: true,
	}),
);

// respond to preflight requests for all routes
app.options("*", cors());

// accept large JSON payloads for base64 uploads
app.use(express.json({ limit: "100mb" }));

const upload = multer({ storage: multer.memoryStorage() });

app.get("/", (_req, res) => {
	res.json({ status: "ok" });
});

app.post("/upload", upload.single("file"), async (req, res) => {
	const file = req.file;
	if (!file)
		return res
			.status(400)
			.json({ error: "No file uploaded (field name: file)" });

	try {
		const workbook = XLSX.read(file.buffer, { type: "buffer" });
		const sheetName = workbook.SheetNames[0];
		const sheet = workbook.Sheets[sheetName];
		const objects = XLSX.utils.sheet_to_json<
			Record<string, string | number | null>
		>(sheet, { range: 1, defval: null });
		await saveRows(objects);

		return res.json({ rows: objects, count: objects.length });
	} catch (err) {
		console.error("Failed to parse XLSX:", err);
		return res.status(500).json({ error: "Failed to parse XLSX" });
	}
});

// Accept uploads as JSON { filename, type, size, data: base64 }
app.post("/upload-json", async (req, res) => {
	const { filename, type, size, data } = req.body as {
		filename?: string;
		type?: string;
		size?: number;
		data?: string;
	};
	if (!data) return res.status(400).json({ error: "No base64 data provided" });

	try {
		const buffer = Buffer.from(data, "base64");
		const workbook = XLSX.read(buffer, { type: "buffer" });
		const sheetName = workbook.SheetNames[0];
		const sheet = workbook.Sheets[sheetName];
		const objects = XLSX.utils.sheet_to_json<
			Record<string, string | number | null>
		>(sheet, { range: 1, defval: null });
		await saveRows(objects);

		const columns = objects.length ? Object.keys(objects[0]) : [];
		return res.json({ rows: objects, count: objects.length, columns });
	} catch (err) {
		console.error("Failed to parse XLSX from base64 JSON:", err);
		return res.status(500).json({ error: "Failed to parse XLSX" });
	}
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () =>
	console.log(`API listening on http://localhost:${PORT}`),
);
