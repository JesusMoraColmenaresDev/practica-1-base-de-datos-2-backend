import "dotenv/config";
import express from "express";
import cors from "cors";
import multer from "multer";
import XLSX from "xlsx";
import { processImport, saveRows } from "./prismaService";

const MONTH_TO_NUMBER: Record<string, string> = {
	JAN: "01",
	FEB: "02",
	MAR: "03",
	APR: "04",
	MAY: "05",
	JUN: "06",
	JUL: "07",
	AUG: "08",
	SEP: "09",
	OCT: "10",
	NOV: "11",
	DEC: "12",
};

function extractReportTimeDate(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;

	const normalized = value.replace(/\s+/g, " ").trim();
	const match = normalized.match(/Data\s+As\s+Of\s+([A-Za-z]+)\s+(\d{4})/i);
	if (!match) return undefined;

	const monthKey = match[1].slice(0, 3).toUpperCase();
	const month = MONTH_TO_NUMBER[monthKey];
	if (!month) return undefined;

	const year = match[2];
	return `${year}-${month}-01`;
}

function getSheetHeaderMetadata(sheet: XLSX.WorkSheet): {
	rawHeader?: string;
	reportTimeDate?: string;
} {
	const rawCell = sheet["A1"]?.v ?? sheet["A1"]?.w;
	const rawHeader =
		rawCell === undefined || rawCell === null ? undefined : String(rawCell);
	const reportTimeDate = extractReportTimeDate(rawHeader);

	return { rawHeader, reportTimeDate };
}

function getTodayIsoDate(): string {
	return new Date().toISOString().split("T")[0];
}

function resolveEffectiveTimeDate(
	reportTimeDate?: string,
	requestTimeDate?: string,
): { timeDate: string; source: "report_header" | "request" | "fallback_now" } {
	if (reportTimeDate) {
		return { timeDate: reportTimeDate, source: "report_header" };
	}

	if (requestTimeDate && requestTimeDate.trim() !== "") {
		return { timeDate: requestTimeDate.trim(), source: "request" };
	}

	return { timeDate: getTodayIsoDate(), source: "fallback_now" };
}

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
		const { rawHeader, reportTimeDate } = getSheetHeaderMetadata(sheet);
		const resolvedDate = resolveEffectiveTimeDate(reportTimeDate, timeDate);

		if (rawHeader) {
			console.log("Report header cell A1:", rawHeader);
		}

		if (reportTimeDate) {
			console.log("Extracted report timeDate from header:", reportTimeDate);
		}

		console.log(
			`Using timeDate for processing: ${resolvedDate.timeDate} (source: ${resolvedDate.source})`,
		);

		const objects = XLSX.utils.sheet_to_json<
			Record<string, string | number | null>
		>(sheet, { range: 1, defval: null });
		const result = await saveRows(objects, {
			processAfterInsert: true,
			timeDate: resolvedDate.timeDate,
		});

		return res.json({
			rows: objects,
			count: objects.length,
			inserted: result.inserted,
			errors: result.errors,
			processed: result.processed,
			timeDate: result.timeDate,
			reportTimeDate,
			reportHeader: rawHeader,
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
		const { rawHeader, reportTimeDate } = getSheetHeaderMetadata(sheet);
		const resolvedDate = resolveEffectiveTimeDate(reportTimeDate, timeDate);

		if (rawHeader) {
			console.log("Report header cell A1:", rawHeader);
		}

		if (reportTimeDate) {
			console.log("Extracted report timeDate from header:", reportTimeDate);
		}

		console.log(
			`Using timeDate for processing: ${resolvedDate.timeDate} (source: ${resolvedDate.source})`,
		);

		const objects = XLSX.utils.sheet_to_json<
			Record<string, string | number | null>
		>(sheet, { range: 1, defval: null });
		const result = await saveRows(objects, {
			processAfterInsert: true,
			timeDate: resolvedDate.timeDate,
		});

		const columns = objects.length ? Object.keys(objects[0]) : [];
		return res.json({
			rows: objects,
			count: objects.length,
			columns,
			inserted: result.inserted,
			errors: result.errors,
			processed: result.processed,
			timeDate: result.timeDate,
			reportTimeDate,
			reportHeader: rawHeader,
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
