import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects } from 'hyparquet';

export type ParquetRow = Record<string, unknown>;

export function toDateString(value: unknown): string {
  if (value instanceof Date) return value.toISOString().split('T')[0];
  return String(value || '').split('T')[0];
}

export function toNumber(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export async function readParquetLastRows(
  filePath: string,
  columns: string[],
  limit = 120
): Promise<ParquetRow[]> {
  const file = await asyncBufferFromFile(filePath);
  const metadata = await parquetMetadataAsync(file);
  const totalRows = Number(metadata.num_rows || 0);

  if (!totalRows) return [];

  const rowStart = Math.max(0, totalRows - limit);

  return parquetReadObjects({
    file,
    columns,
    rowStart,
    rowEnd: totalRows,
  }) as Promise<ParquetRow[]>;
}

export async function readParquetAllRows(
  filePath: string,
  columns: string[]
): Promise<ParquetRow[]> {
  const file = await asyncBufferFromFile(filePath);
  return parquetReadObjects({
    file,
    columns,
  }) as Promise<ParquetRow[]>;
}
