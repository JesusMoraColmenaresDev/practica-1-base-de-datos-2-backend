import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import XLSX from "xlsx";
import { processImport, saveRows } from "./prismaService";

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
	const { timeDate } = (req.body ?? {}) as { timeDate?: string };
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
		const result = await saveRows(objects, {
			processAfterInsert: true,
			timeDate,
		});

		return res.json({
			rows: objects,
			count: objects.length,
			inserted: result.inserted,
			errors: result.errors,
		});
	} catch (err) {
		console.error("Failed to parse XLSX:", err);
		return res.status(500).json({ error: "Failed to parse XLSX" });
	}
});

// Accept uploads as JSON { filename, type, size, data: base64 }
app.post("/upload-json", async (req, res) => {
	const { filename, type, size, data, timeDate } = req.body as {
		filename?: string;
		type?: string;
		size?: number;
		data?: string;
		timeDate?: string;
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
		const result = await saveRows(objects, {
			processAfterInsert: true,
			timeDate,
		});

		const columns = objects.length ? Object.keys(objects[0]) : [];
		return res.json({
			rows: objects,
			count: objects.length,
			columns,
			inserted: result.inserted,
			errors: result.errors,
		});
	} catch (err) {
		console.error("Failed to parse XLSX from base64 JSON:", err);
		return res.status(500).json({ error: "Failed to parse XLSX" });
	}
});

app.post("/process-import", async (req, res) => {
	const { timeDate } = (req.body ?? {}) as { timeDate?: string };

	try {
		const result = await processImport(timeDate);
		return res.json(result);
	} catch (err) {
		const message =
			err instanceof Error ? err.message : "Failed to process import";
		return res.status(400).json({ error: message });
	}
});

const PORT = Number(process.env.PORT ?? 4000);

function startServer(port: number): void {
	const server = app.listen(port, () =>
		console.log(`API listening on http://localhost:${port}`),
	);

	server.on("error", (error: NodeJS.ErrnoException) => {
		if (error.code === "EADDRINUSE") {
			const nextPort = port + 1;
			console.warn(
				`Port ${port} is in use, retrying on http://localhost:${nextPort}`,
			);
			startServer(nextPort);
			return;
		}

		throw error;
	});
}

startServer(PORT);
