import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "./generated/prisma";

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
	throw new Error("Missing DATABASE_URL environment variable");
}

const adapter = new PrismaPg({ connectionString: databaseUrl });
const prisma = new PrismaClient({ adapter });

export interface SaveRowsResult {
	inserted: number;
	errors: Array<{ index: number; reason: string }>;
	processed?: boolean;
	timeDate?: string;
}

export interface ProcessImportResult {
	timeDate: string;
	processed: boolean;
}

interface SaveRowsOptions {
	chunkSize?: number;
	processAfterInsert?: boolean;
	timeDate?: string;
}

function resolveImportDate(timeDate?: string): string {
	const selectedDate =
		timeDate && timeDate.trim() !== ""
			? timeDate.trim()
			: new Date().toISOString().split("T")[0];

	if (!/^\d{4}-\d{2}-\d{2}$/.test(selectedDate)) {
		throw new Error("timeDate must use format YYYY-MM-DD");
	}

	const parsedDate = new Date(`${selectedDate}T00:00:00.000Z`);
	if (Number.isNaN(parsedDate.getTime())) {
		throw new Error("timeDate is not a valid date");
	}

	return selectedDate;
}

function normalizeKey(value: string): string {
	return value.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function getValue(row: Record<string, unknown>, aliases: string[]): unknown {
	for (const alias of aliases) {
		const direct = row[alias];
		if (
			direct !== undefined &&
			direct !== null &&
			String(direct).trim() !== ""
		) {
			return direct;
		}
	}

	const normalizedRow: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(row)) {
		normalizedRow[normalizeKey(key)] = value;
	}

	for (const alias of aliases) {
		const candidate = normalizedRow[normalizeKey(alias)];
		if (
			candidate !== undefined &&
			candidate !== null &&
			String(candidate).trim() !== ""
		) {
			return candidate;
		}
	}

	return null;
}

function asText(value: unknown): string | null {
	if (value === null || value === undefined) return null;
	const normalized = String(value).trim();
	return normalized === "" ? null : normalized;
}

function asInt(value: unknown): number | null {
	if (value === null || value === undefined || String(value).trim() === "")
		return null;
	const normalized = String(value).replace(/,/g, "").trim();
	const parsed = Number.parseInt(normalized, 10);
	return Number.isNaN(parsed) ? null : parsed;
}

function asFloat(value: unknown): number | null {
	if (value === null || value === undefined || String(value).trim() === "")
		return null;
	const normalized = String(value).replace(/%/g, "").replace(/,/g, "").trim();
	const parsed = Number.parseFloat(normalized);
	return Number.isNaN(parsed) ? null : parsed;
}

export async function saveRows(
	rows: Record<string, unknown>[],
	options: SaveRowsOptions = {},
): Promise<SaveRowsResult> {
	if (!Array.isArray(rows)) {
		throw new Error("rows must be an array");
	}

	const chunkSize = options.chunkSize ?? 2000;
	const processAfterInsert = options.processAfterInsert ?? false;
	const selectedDate = processAfterInsert
		? resolveImportDate(options.timeDate)
		: undefined;

	const mappedRows = rows.map((row) => ({
		factory_name: asText(
			getValue(row, ["Factory Name", "factory_name", "Factory"]),
		),
		factory_type: asText(
			getValue(row, ["Factory Type", "factory_type", "Type"]),
		),
		product_type: asText(
			getValue(row, [
				"Product Type",
				"Product Type Type",
				"product_type",
				"Product",
			]),
		),
		brand: asText(
			getValue(row, ["Brand", "Nike, Inc. Brand(s)", "Brand(s)", "brand"]),
		),
		event: asText(getValue(row, ["Event", "Events", "event"])),
		supplier_group: asText(
			getValue(row, ["Supplier Group", "supplier_group", "Supplier"]),
		),
		address: asText(getValue(row, ["Address", "address"])),
		city: asText(getValue(row, ["City", "city"])),
		state: asText(getValue(row, ["State", "state"])),
		postal_code: asText(
			getValue(row, ["Postal Code", "postal_code", "Zip", "ZIP Code"]),
		),
		country_region: asText(
			getValue(row, [
				"Country/Region",
				"Country / Region",
				"Country Region",
				"country_region",
				"Country",
				"country",
			]),
		),
		region: asText(getValue(row, ["Region", "region"])),
		total_workers: asInt(
			getValue(row, ["Total Workers", "total_workers", "Workers Total"]),
		),
		line_workers: asInt(
			getValue(row, ["Line Workers", "line_workers", "Workers Line"]),
		),
		pct_female: asFloat(
			getValue(row, [
				"% Female",
				"% Female Workers",
				"pct_female",
				"Female %",
				"Female",
			]),
		),
		pct_migrant: asFloat(
			getValue(row, [
				"% Migrant",
				"% Migrant Workers",
				"pct_migrant",
				"Migrant %",
				"Migrant",
			]),
		),
	}));

	const sampleSize = Math.min(5, rows.length);
	if (sampleSize > 0) {
		const detectedKeys = Array.from(
			new Set(rows.flatMap((row) => Object.keys(row))),
		);
		console.log("Detected input headers:", detectedKeys);
		console.log("Incoming rows sample (first 5):", rows.slice(0, sampleSize));
		console.log(
			"Mapped rows sample for staging_imap (first 5):",
			mappedRows.slice(0, sampleSize),
		);
	}

	console.log(
		`Inserting ${mappedRows.length} rows into staging_imap in chunks of ${chunkSize}...`,
	);

	let inserted = 0;
	const errors: Array<{ index: number; reason: string }> = [];

	await prisma.$transaction(async (tx) => {
		for (let index = 0; index < mappedRows.length; index += chunkSize) {
			const chunk = mappedRows.slice(index, index + chunkSize);

			try {
				const result = await tx.staging_imap.createMany({
					data: chunk,
				});
				inserted += result.count ?? 0;
			} catch (error) {
				errors.push({
					index,
					reason: error instanceof Error ? error.message : String(error),
				});
			}
		}

		if (processAfterInsert) {
			if (errors.length > 0) {
				throw new Error(
					"Import processing skipped because one or more createMany chunks failed",
				);
			}

			await tx.$executeRawUnsafe(
				`SELECT public.process_import($1::date)`,
				selectedDate,
			);
		}
	});

	console.log(
		`Staging insert complete. Inserted: ${inserted}. Errors: ${errors.length}.`,
	);

	if (processAfterInsert) {
		console.log(`process_import completed for date ${selectedDate}`);
	}

	return {
		inserted,
		errors,
		processed: processAfterInsert,
		timeDate: selectedDate,
	};
}

export async function processImport(
	timeDate?: string,
): Promise<ProcessImportResult> {
	const selectedDate = resolveImportDate(timeDate);

	await prisma.$executeRawUnsafe(
		`SELECT public.process_import($1::date)`,
		selectedDate,
	);

	return {
		timeDate: selectedDate,
		processed: true,
	};
}
