export async function saveRows(rows: Record<string, string | number | null>[]): Promise<void> {
    for (const row of rows) {
        const factory = row['Factory Name'];
        const total = Number(row['Total Workers'] ?? 0);
        console.log(`Saving factory: ${factory} with total workers: ${total}`);
    }
}
