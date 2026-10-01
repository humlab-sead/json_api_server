import SdfSchema from "./SdfSchema.class.js";
import { fetchRows, arrayType } from "./SdfRows.js";
import { loadWorkbook, readCell, plainValue, plainRows, CELL } from "./SdfWorkbookReader.js";
import { SdfError, quoteIdent, rowHash, canonicalValue, canonicalCsv, sha256Hex } from "./SdfCommon.js";

/**
 * Import stages 1-4 (spec §10): structural check, cell coercion, referential
 * resolution and the three-way diff. It writes nothing, to any database. For
 * now, an import only edits rows that exist (SUPPORTED_OPERATIONS).
 *
 * Every error and warning is anchored to a sheet, and to a cell where there is
 * one. Processing halts at the first stage that produces an error, so the
 * report never mixes consequences with causes.
 *
 * All database reads happen in one REPEATABLE READ, READ ONLY transaction. A
 * caller that needs to keep reading from the same snapshot after validation
 * (the change-request generator) passes `withSnapshot`, which runs before the
 * transaction ends.
 */

const SUPPORTED_MAJOR = 2;
const SUPPORTED_MINOR = 0;
const MACHINE_SHEETS = ["_sdf_meta", "_sdf_columns", "_sdf_baseline"];
const TOKEN = /^NEW-[A-Za-z0-9_-]{1,32}$/;
const TOKEN_LIKE = /^NEW-/i;
const STRICT_INTEGER = /^[+-]?\d+$/;
const STRICT_DECIMAL = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
const COMMA_DECIMAL = /^[+-]?\d+,\d+$/;
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;
const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}(:?\d{2})?)?$/;
const UUID = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const RANGE = /^(empty|[[(][^,]*,[^,]*[\])])$/;
const MAX_REPORTED_PER_CODE = 200;

//Which changes to site data an import may make. The first version only edits rows
//that exist (spec §10, "Scope"). The insert and delete paths are kept, switched
//off: before either is switched on, the review's findings on it must be fixed -
//inserts: ID allocation and reservations, sequences, materialised UUIDs, applying
//a workbook twice, self-references, the follow-up; deletes: guards against rows
//added under a deleted row since generation, blocking, the legacy dendro tables,
//foreign keys from other schemas.
const SUPPORTED_OPERATIONS = { insert: false, delete: false };

class Report {
    constructor() {
        this.ok = false;
        this.stage_reached = 0;
        this.errors = [];
        this.warnings = [];
        this.suppressed = {};
        this.export = null;
        this.database_changes_since_export = [];
        this.change_set = null;
        this.summary = null;
    }

    error(stage, code, message, where = {}) {
        this._add(this.errors, { stage, code, ...where, message });
    }

    warning(stage, code, message, where = {}) {
        this._add(this.warnings, { stage, code, ...where, message });
    }

    _add(list, issue) {
        const n = list.filter(i => i.code === issue.code).length;
        if (n >= MAX_REPORTED_PER_CODE) {
            this.suppressed[issue.code] = (this.suppressed[issue.code] || 0) + 1;
            return;
        }
        list.push(issue);
    }

    hasErrors() {
        return this.errors.length > 0;
    }
}

export default class SdfValidator {

    constructor(app) {
        this.app = app;
    }

    /**
     * @param {Buffer} buffer the uploaded workbook
     * @param {object} opts
     * @param {function} opts.withSnapshot async (ctx) => any, run inside the
     *   read-only snapshot after a successful validation
     * @returns {Promise<{ report: Report, ctx: object, extra: any }>}
     */
    async validate(buffer, opts = {}) {
        const report = new Report();
        let wb;
        try {
            wb = await loadWorkbook(buffer);
        }
        catch (err) {
            if (err instanceof SdfError) {
                report.stage_reached = 1;
                report.error(1, err.code, err.message);
                return { report, ctx: null };
            }
            throw err;
        }

        const client = await this.app.pgPool.connect();
        try {
            await client.query("begin isolation level repeatable read read only");
            const schema = await SdfSchema.load(client);
            const ctx = { client, schema, report, wb };
            let extra;

            const stages = [
                () => this._structural(ctx),
                () => this._coerce(ctx),
                () => this._resolve(ctx),
                () => this._diff(ctx),
            ];
            for (let i = 0; i < stages.length; i++) {
                report.stage_reached = i + 1;
                await stages[i]();
                if (report.hasErrors()) break;
            }
            report.ok = !report.hasErrors();
            if (report.ok && opts.withSnapshot) {
                extra = await opts.withSnapshot(ctx);
            }
            await client.query("rollback");
            return { report, ctx, extra };
        }
        catch (err) {
            await client.query("rollback").catch(() => {});
            throw err;
        }
        finally {
            client.release();
        }
    }

    //===================================================== stage 1: structure

    async _structural(ctx) {
        const { wb, report, client, schema } = ctx;

        const missing = MACHINE_SHEETS.filter(name => !wb.getWorksheet(name));
        if (missing.length) {
            report.error(1, "not_sdf",
                "This workbook does not carry the hidden sheets an SDF export has " +
                `(${missing.join(", ")}). Only a workbook downloaded from SEAD as SDF can be imported.`);
            return;
        }

        //_sdf_meta
        const metaRows = plainRows(wb.getWorksheet("_sdf_meta")).slice(1);
        const meta = new Map(metaRows.map(r => [r[0], r[1] ?? null]));
        ctx.meta = meta;
        report.export = {
            export_id: meta.get("export_id"),
            exported_at: meta.get("exported_at"),
            exported_by: meta.get("exported_by"),
            site_ids: meta.get("site_ids"),
            database_name: meta.get("database_name"),
            database_release: meta.get("database_release"),
            sdf_version: meta.get("sdf_version"),
        };

        //checksum over the machine sheets exactly as read (§9)
        const columnRows = plainRows(wb.getWorksheet("_sdf_columns"));
        const baselineRows = plainRows(wb.getWorksheet("_sdf_baseline"));
        const checksum = sha256Hex(canonicalCsv(columnRows) + canonicalCsv(baselineRows));
        if (checksum !== meta.get("checksum")) {
            report.error(1, "checksum_mismatch",
                "The hidden sheets _sdf_columns or _sdf_baseline have been changed. They must not be edited; " +
                "start again from the file as downloaded, or from a fresh export.", { sheet: "_sdf_meta" });
            return;
        }

        //format version (§9)
        const version = /^SDF\/(\d+)\.(\d+)$/.exec(meta.get("sdf_version") || "");
        if (!version) {
            report.error(1, "unsupported_version", `Unrecognised format version "${meta.get("sdf_version")}".`, { sheet: "_sdf_meta" });
            return;
        }
        const [major, minor] = [Number(version[1]), Number(version[2])];
        if (major !== SUPPORTED_MAJOR || minor > SUPPORTED_MINOR) {
            report.error(1, "unsupported_version",
                `The workbook is ${meta.get("sdf_version")}; this importer reads SDF/${SUPPORTED_MAJOR}.0` +
                (major === SUPPORTED_MAJOR ? " and earlier minor versions." : "."), { sheet: "_sdf_meta" });
            return;
        }

        //database identity (§9)
        const live = (await client.query("select current_database() as name")).rows[0];
        let liveSystemId = null;
        try {
            await client.query("savepoint sdf_id");
            liveSystemId = (await client.query("select system_identifier::text as id from pg_control_system()")).rows[0].id;
            await client.query("release savepoint sdf_id");
        }
        catch (err) {
            await client.query("rollback to savepoint sdf_id");
        }
        const wbSystemId = meta.get("database_system_identifier");
        if (meta.get("database_name") !== live.name || (wbSystemId && liveSystemId && String(wbSystemId) !== liveSystemId)) {
            report.error(1, "database_mismatch",
                `This workbook was exported from ${meta.get("database_name")}` +
                (wbSystemId ? ` (cluster ${wbSystemId})` : "") +
                ` and can only be imported there; this is ${live.name}` +
                (liveSystemId ? ` (cluster ${liveSystemId})` : "") + ". Its IDs mean nothing in another database.",
                { sheet: "_sdf_meta" });
            return;
        }

        //Sqitch: already submitted, and what changed since export
        await this._checkSqitch(ctx);
        if (report.hasErrors()) return;

        //_sdf_columns: what was exported, sheet by sheet
        const header = columnRows[0] || [];
        const col = name => header.indexOf(name);
        ctx.exported = new Map();
        for (const r of columnRows.slice(1)) {
            const sheet = r[col("sheet")];
            if (!ctx.exported.has(sheet)) {
                ctx.exported.set(sheet, { table: r[col("table")], role: r[col("sheet_role")], columns: new Map(), dataKeys: [] });
            }
            const entry = ctx.exported.get(sheet);
            const key = r[col("key")];
            entry.columns.set(key, { kind: r[col("kind")], source: r[col("source")] });
            if (r[col("kind")] === "data") entry.dataKeys.push(key);
        }

        //_sdf_baseline
        ctx.baseline = new Map();
        for (const r of baselineRows.slice(1)) {
            ctx.baseline.set(`${r[0]}:${r[1]}`, { hash: r[2], shared: r[3] === true || r[3] === "true" });
        }
        ctx.bundleSiteIds = String(meta.get("site_ids") || "").split(",").map(Number).filter(Boolean);

        //bind sheets to tables (§4) and headers to columns (§5)
        ctx.bindings = [];
        ctx.schemaProposals = [];
        for (const ws of wb.worksheets) {
            const name = ws.name;
            if (MACHINE_SHEETS.includes(name) || name === "_sdf_lists" || name === "README" ||
                name.startsWith("README_") || name.startsWith("view_")) {
                continue;
            }
            if (name.startsWith("_")) {
                report.warning(1, "reserved_sheet_ignored", `Sheet "${name}" starts with "_", which is reserved; it was ignored.`, { sheet: name });
                continue;
            }
            const exported = ctx.exported.get(name);
            if (exported) {
                const table = schema.tables.get(exported.table);
                if (!table) {
                    report.error(1, "table_gone", `The table behind sheet "${name}" (${exported.table}) no longer exists in the database.`, { sheet: name });
                    continue;
                }
                this._bind(ctx, ws, table, "exported", exported.role, exported);
                continue;
            }
            const table = schema.tableForSheet(name);
            const role = table && schema.roleOf(table.name);
            if (table && role) {
                const deprecated = schema.deprecationOf(table.name);
                if (deprecated) {
                    report.warning(1, "deprecated_sheet_ignored", `Sheet "${name}" is a deprecated table; it was ignored.`, { sheet: name });
                    continue;
                }
                this._bind(ctx, ws, table, "added", role, null);
                continue;
            }
            this._bindProposedTable(ctx, ws);
        }
    }

    async _checkSqitch(ctx) {
        const { client, meta, report } = ctx;
        try {
            await client.query("savepoint sdf_sqitch");
            const exportId = meta.get("export_id");
            if (exportId) {
                const res = await client.query(
                    "select change, project, committed_at from sqitch.changes where note like $1 order by committed_at limit 1",
                    [`%sdf-export:${exportId}%`]);
                if (res.rows.length) {
                    const c = res.rows[0];
                    report.error(1, "already_submitted",
                        `The changes in this workbook were already deployed as ${c.project}:${c.change}. ` +
                        "Submitting it again would repeat them. Start from a fresh export.",
                        { sheet: "_sdf_meta" });
                }
            }
            for (const [key, value] of meta) {
                if (!key || !key.startsWith("sqitch:") || !value) continue;
                const project = key.slice("sqitch:".length);
                const changeId = String(value).split(" · ")[1];
                if (!changeId) continue;
                const res = await client.query(`
                    select c.change, c.change_id, c.committed_at::text as committed_at
                    from sqitch.changes c
                    where c.project = $1
                      and c.committed_at > (select committed_at from sqitch.changes where change_id = $2)
                    order by c.committed_at`, [project, changeId]);
                const known = await client.query("select 1 from sqitch.changes where change_id = $1", [changeId]);
                if (!known.rows.length) {
                    report.warning(1, "sqitch_change_missing",
                        `The database no longer has the ${project} change this workbook was exported after (${value}). It may have been reverted.`,
                        { sheet: "_sdf_meta" });
                }
                for (const r of res.rows) {
                    report.database_changes_since_export.push({ project, change: r.change, change_id: r.change_id, committed_at: r.committed_at });
                }
            }
            await client.query("release savepoint sdf_sqitch");
        }
        catch (err) {
            await client.query("rollback to savepoint sdf_sqitch");
        }
    }

    /**
     * Binds a worksheet to a table: every header to a column kind. Missing data
     * columns of an exported sheet are an error (§5); headers the table does not
     * have become schema proposals (§10).
     */
    _bind(ctx, ws, table, mode, role, exported) {
        const { report, schema } = ctx;
        const sheet = ws.name;
        const headers = this._headers(ctx, ws);
        if (headers === null) return;

        const binding = {
            ws, sheet, table, mode, role, exported,
            action: null, data: new Map(), label: new Map(), system: new Map(), proposed: new Map(),
            baseKeys: exported ? exported.dataKeys.filter(k => schema.column(table, k)) : [],
        };
        for (const { key, index } of headers) {
            if (key === "_action") { binding.action = index; continue; }
            if (key.endsWith(":label")) {
                const fkCol = schema.column(table, key.slice(0, -":label".length));
                if (fkCol && fkCol.fk) { binding.label.set(fkCol.name, index); continue; }
            }
            const column = schema.column(table, key);
            if (column && !column.generated) {
                (column.system ? binding.system : binding.data).set(key, index);
                continue;
            }
            if (column && column.generated) {
                report.warning(1, "generated_column_ignored", `Column ${key} is computed by the database; it was ignored.`, { sheet, cell: `${ws.getColumn(index).letter}1` });
                continue;
            }
            if (key.startsWith("_") || key.includes(":")) {
                report.warning(1, "reserved_column_ignored", `Column "${key}" uses a reserved name; it was ignored.`, { sheet, cell: `${ws.getColumn(index).letter}1` });
                continue;
            }
            binding.proposed.set(key, index);
        }

        if (exported) {
            for (const key of exported.dataKeys) {
                if (!schema.column(table, key)) {
                    report.warning(1, "column_gone", `Column ${key} has been removed from ${table.name} since export; its values are ignored.`, { sheet });
                    continue;
                }
                if (!binding.data.has(key)) {
                    report.error(1, "missing_column",
                        `Column ${key} has been deleted or renamed. Every column of an exported sheet must stay, with its name in row 1 unchanged; hide columns you do not need instead.`,
                        { sheet });
                }
            }
            for (const [key] of binding.data) {
                if (!exported.dataKeys.includes(key)) {
                    report.warning(1, "column_added_since_export", `Column ${key} was added to ${table.name} after this workbook was exported.`, { sheet });
                }
            }
        }
        else if (!binding.data.has(table.pk)) {
            report.error(1, "missing_column", `A sheet added for ${table.name} needs its ID column, ${table.pk}.`, { sheet });
        }

        if (binding.proposed.size && role === "owned") {
            for (const [key, index] of binding.proposed) {
                ctx.schemaProposals.push({ kind: "schema", type: "column", table: table.name, sheet, column: key, index });
            }
        }
        else if (binding.proposed.size) {
            for (const [key, index] of binding.proposed) {
                report.warning(1, "unknown_column_ignored",
                    `Column "${key}" is not a column of ${table.name}. New columns can only be proposed for site data, not for shared lists; it was ignored.`,
                    { sheet, cell: `${ws.getColumn(index).letter}1` });
            }
            binding.proposed.clear();
        }
        ctx.bindings.push(binding);
    }

    /** A sheet naming no table: a proposed new table (§10). */
    _bindProposedTable(ctx, ws) {
        const { report, schema } = ctx;
        const sheet = ws.name;
        const headers = this._headers(ctx, ws);
        if (headers === null) return;

        const ownedKeys = schema.ownedKeyColumns();
        const attachments = headers.filter(h => ownedKeys.has(h.key));
        const pk = headers.find(h => /_id$/.test(h.key) && !ownedKeys.has(h.key) && h.key !== "_action");
        const problems = [];
        if (!pk) problems.push("an ID column of its own whose name ends in _id");
        if (!attachments.length) problems.push(`a column linking each row to this site's data (one of ${[...ownedKeys.keys()].slice(0, 6).join(", ")}, …)`);
        if (problems.length) {
            report.error(1, "proposed_table_unattached",
                `Sheet "${sheet}" is not a SEAD table, so it is read as a proposed new table. A proposed table needs ${problems.join(" and ")}.`,
                { sheet });
            return;
        }
        ctx.schemaProposals.push({
            kind: "schema", type: "table", sheet, ws,
            pk: pk.key,
            attachments: attachments.map(a => ({ column: a.key, table: ownedKeys.get(a.key), index: a.index })),
            headers,
        });
    }

    _headers(ctx, ws) {
        const { report } = ctx;
        const row = ws.getRow(1);
        const headers = [];
        const seen = new Map();
        for (let c = 1; c <= ws.columnCount; c++) {
            const value = plainValue(row.getCell(c));
            if (value === null) continue;
            if (typeof value !== "string") {
                report.error(1, "bad_header", `The header in row 1 must be a column name, not ${JSON.stringify(value)}.`,
                    { sheet: ws.name, cell: `${ws.getColumn(c).letter}1` });
                continue;
            }
            const key = value;
            if (seen.has(key)) {
                report.error(1, "duplicate_column", `Column ${key} appears twice (also ${ws.getColumn(seen.get(key)).letter}1).`,
                    { sheet: ws.name, cell: `${ws.getColumn(c).letter}1` });
                continue;
            }
            seen.set(key, c);
            headers.push({ key, index: c });
        }
        return headers;
    }

    //======================================================= stage 2: cells

    _coerce(ctx) {
        const { report, schema } = ctx;
        ctx.records = [];
        for (const b of ctx.bindings) {
            const { ws, sheet, table } = b;
            const keyColumns = this._keyColumns(schema, table);

            ws.eachRow({ includeEmpty: false }, (row, rowNumber) => {
                if (rowNumber === 1) return;
                const cellAt = index => readCell(row.getCell(index));
                const where = cell => ({ sheet, cell: cell.address });

                const record = {
                    binding: b, table: table.name, sheet, row: rowNumber,
                    action: null, values: new Map(), cells: new Map(), labels: new Map(), proposed: new Map(),
                };

                let anyData = false;
                for (const [key, index] of b.data) {
                    const cell = cellAt(index);
                    record.cells.set(key, cell.address);
                    if (cell.kind !== CELL.BLANK) anyData = true;
                    const column = schema.column(table, key);
                    const value = this._coerceCell(report, cell, column, keyColumns.has(key), where(cell));
                    record.values.set(key, value);
                }
                let anyOther = false;
                for (const [key, index] of b.label) {
                    const cell = cellAt(index);
                    if (cell.kind === CELL.BLANK) continue;
                    anyOther = true;
                    record.labels.set(key, { text: cell.kind === CELL.NUMBER ? String(cell.value) : String(cell.value), address: cell.address });
                }
                for (const [key, index] of b.proposed) {
                    const cell = cellAt(index);
                    if (cell.kind === CELL.BLANK) continue;
                    anyData = true;
                    if (cell.kind === CELL.FORMULA || cell.kind === CELL.ERROR) {
                        report.error(2, "formula", `${cell.address} holds a formula or an error value; data cells must hold values.`, where(cell));
                        continue;
                    }
                    record.proposed.set(key, { value: cell.value instanceof Date ? cell.value.toISOString().slice(0, 10) : cell.value, address: cell.address });
                }
                if (b.action !== null) {
                    const cell = cellAt(b.action);
                    if (cell.kind !== CELL.BLANK) {
                        anyOther = true;
                        if (cell.kind === CELL.STRING && cell.value.trim().toLowerCase() === "delete") {
                            record.action = "delete";
                        }
                        else {
                            report.error(2, "bad_action", `_action must be empty or "delete", not ${JSON.stringify(cell.value)}.`, where(cell));
                        }
                    }
                }

                if (!anyData) {
                    if (anyOther) {
                        report.warning(2, "row_ignored", `Row ${rowNumber} has no data in its data columns and was ignored.`, { sheet, cell: `A${rowNumber}` });
                    }
                    return;
                }

                const pk = record.values.get(table.pk);
                record.pk = pk === null || pk === undefined
                    ? { kind: "blank" }
                    : (typeof pk === "string" && TOKEN.test(pk) ? { kind: "token", value: pk } : { kind: "id", value: pk });
                ctx.records.push(record);
            });
        }

        //rows of proposed tables: their values are carried as proposal content
        for (const p of ctx.schemaProposals) {
            if (p.type !== "table") continue;
            p.rows = [];
            p.ws.eachRow({ includeEmpty: false }, (row, rowNumber) => {
                if (rowNumber === 1) return;
                const values = {};
                let any = false;
                for (const h of p.headers) {
                    const cell = readCell(row.getCell(h.index));
                    if (cell.kind === CELL.BLANK) continue;
                    if (cell.kind === CELL.FORMULA || cell.kind === CELL.ERROR) {
                        report.error(2, "formula", `${cell.address} holds a formula or an error value; data cells must hold values.`, { sheet: p.sheet, cell: cell.address });
                        continue;
                    }
                    any = true;
                    values[h.key] = cell.value instanceof Date ? cell.value.toISOString().slice(0, 10) : cell.value;
                }
                if (!any) return;
                const pkValue = values[p.pk];
                if (pkValue !== undefined && !(typeof pkValue === "string" && TOKEN.test(pkValue))) {
                    report.error(2, "proposed_table_id",
                        `Rows of a proposed table are all new: leave ${p.pk} empty or give it a NEW- name, not ${JSON.stringify(pkValue)}.`,
                        { sheet: p.sheet, cell: `${p.ws.getColumn(p.headers.find(h => h.key === p.pk).index).letter}${rowNumber}` });
                }
                p.rows.push({ row: rowNumber, values });
            });
        }

        this._checkSupportedOperations(ctx);
    }

    /**
     * §10 scope: new rows and deletes in site data are refused while the
     * operation is not supported. Shared lists are unaffected, since a new or
     * deleted row there is only ever a proposal.
     */
    _checkSupportedOperations(ctx) {
        const { report, schema } = ctx;
        const addedSheets = new Set();
        for (const b of ctx.bindings) {
            if (b.role !== "owned" || b.mode !== "added" || SUPPORTED_OPERATIONS.insert) continue;
            if (!ctx.records.some(r => r.binding === b)) continue;
            addedSheets.add(b.sheet);
            report.error(2, "insert_not_supported",
                `Sheet "${b.sheet}" adds rows to a table this file did not contain. Adding rows is not supported yet; ` +
                "only changes to rows that already exist can be imported. Remove the sheet.", { sheet: b.sheet });
        }
        for (const r of ctx.records) {
            if (r.binding.role !== "owned" || addedSheets.has(r.sheet)) continue;
            const pkCell = r.cells.get(schema.table(r.table).pk);
            if (r.pk.kind !== "id" && !SUPPORTED_OPERATIONS.insert) {
                report.error(2, "insert_not_supported",
                    `Row ${r.row} is a new row. Adding rows is not supported yet; only changes to rows that already exist can be imported. Remove the row.`,
                    { sheet: r.sheet, cell: pkCell || `A${r.row}` });
            }
            else if (r.action === "delete" && !SUPPORTED_OPERATIONS.delete) {
                report.error(2, "delete_not_supported",
                    `Row ${r.row} is marked delete. Deleting rows is not supported yet; clear its _action cell.`,
                    { sheet: r.sheet, cell: `${r.binding.ws.getColumn(r.binding.action).letter}${r.row}` });
            }
        }
    }

    /**
     * §7: the columns a NEW- token may stand in: the primary key, and foreign
     * keys that reference a primary key. Everywhere else "NEW-…" is plain text.
     */
    _keyColumns(schema, table) {
        return new Set([table.pk, ...table.fks.filter(fk => {
            const target = schema.tables.get(fk.parent);
            return target && target.pk === fk.parentColumn;
        }).map(fk => fk.column)]);
    }

    /**
     * §8 strict coercion of one cell to its column's carrier value. Returns the
     * value, or undefined after reporting an error.
     */
    _coerceCell(report, cell, column, isKey, where) {
        const addr = cell.address;
        if (cell.kind === CELL.BLANK) return null;
        if (cell.kind === CELL.FORMULA) {
            report.error(2, "formula", `${addr} holds a formula (=${cell.value}). Data cells must hold values; paste values instead.`, where);
            return undefined;
        }
        if (cell.kind === CELL.ERROR) {
            report.error(2, "error_value", `${addr} holds an error value (${cell.value}).`, where);
            return undefined;
        }
        if (isKey && cell.kind === CELL.STRING && TOKEN_LIKE.test(cell.value)) {
            if (!TOKEN.test(cell.value)) {
                report.error(2, "bad_token", `${addr}: "${cell.value}" is not a valid NEW- name. Use NEW- followed by 1-32 letters, digits, - or _.`, where);
                return undefined;
            }
            return cell.value;
        }

        switch (column.valueKind) {
            case "integer": {
                if (cell.kind === CELL.NUMBER) {
                    if (Number.isInteger(cell.value)) return cell.value;
                    report.error(2, "not_integer", `${addr}: ${column.name} takes a whole number, not ${cell.value}.`, where);
                    return undefined;
                }
                if (cell.kind === CELL.STRING && STRICT_INTEGER.test(cell.value.trim())) {
                    report.warning(2, "number_stored_as_text", `${addr}: "${cell.value}" was stored as text; read as the number ${Number(cell.value)}.`, where);
                    return Number(cell.value.trim());
                }
                break;
            }
            case "decimal": {
                if (cell.kind === CELL.NUMBER) return cell.value;
                if (cell.kind === CELL.STRING) {
                    const t = cell.value.trim();
                    if (STRICT_DECIMAL.test(t)) {
                        report.warning(2, "number_stored_as_text", `${addr}: "${cell.value}" was stored as text; read as the number ${Number(t)}.`, where);
                        return Number(t);
                    }
                    if (COMMA_DECIMAL.test(t)) {
                        report.error(2, "decimal_comma",
                            `${addr}: "${cell.value}" uses a comma and is stored as text, so it could mean ${t.replace(",", ".")} or ${t.replace(",", "")}. Type it as a number (${t.replace(",", ".")}).`, where);
                        return undefined;
                    }
                }
                break;
            }
            case "boolean": {
                if (cell.kind === CELL.BOOLEAN) return cell.value;
                if (cell.kind === CELL.STRING && /^(true|false)$/i.test(cell.value.trim())) {
                    return cell.value.trim().toLowerCase() === "true";
                }
                break;
            }
            default: {
                //text-carried: text, varchar, uuid, date, timestamps, ranges
                let text;
                if (cell.kind === CELL.STRING) {
                    text = cell.value;
                }
                else if (cell.kind === CELL.DATE && (column.typname === "date" || column.typname === "timestamp" || column.typname === "timestamptz")) {
                    const iso = cell.value.toISOString();
                    text = column.typname === "date" ? iso.slice(0, 10) : iso;
                    report.warning(2, "date_cell", `${addr}: Excel stored this as a date; read as ${text}.`, where);
                }
                else if (cell.kind === CELL.DATE) {
                    report.error(2, "date_in_text_column",
                        `${addr}: Excel turned this value into a date. ${column.name} is text; retype the value with a leading apostrophe (e.g. '20-30).`, where);
                    return undefined;
                }
                else if (cell.kind === CELL.NUMBER) {
                    text = String(cell.value);
                    report.warning(2, "number_in_text_column",
                        `${addr}: a number in the text column ${column.name}, read as "${text}". Check that no leading zeros were lost.`, where);
                }
                else if (cell.kind === CELL.BOOLEAN) {
                    report.error(2, "boolean_in_text_column", `${addr}: TRUE/FALSE in the text column ${column.name}.`, where);
                    return undefined;
                }

                const valid =
                    column.typname === "date" ? ISO_DATE.test(text) :
                    column.typname === "timestamptz" || column.typname === "timestamp" ? ISO_TIMESTAMP.test(text) :
                    column.typname === "uuid" ? UUID.test(text) :
                    /range$/.test(column.typname) ? RANGE.test(text) : true;
                if (!valid) {
                    report.error(2, "bad_format", `${addr}: "${text}" is not a valid ${column.pgType}.`, where);
                    return undefined;
                }
                if (column.maxLength && text.length > column.maxLength) {
                    report.error(2, "too_long", `${addr}: ${text.length} characters; ${column.name} holds at most ${column.maxLength}.`, where);
                    return undefined;
                }
                return text;
            }
        }
        const shown = cell.value instanceof Date ? cell.value.toISOString().slice(0, 10) : JSON.stringify(cell.value);
        report.error(2, "bad_value", `${addr}: ${column.name} is ${column.pgType}; ${shown} is not one.`, where);
        return undefined;
    }

    //=================================================== stage 3: references

    async _resolve(ctx) {
        const { report, schema, client } = ctx;
        ctx.tokens = new Map();   //table -> Map(token -> record)
        ctx.byId = new Map();     //table -> Map(id -> record)
        const tableMap = (m, t) => { if (!m.has(t)) m.set(t, new Map()); return m.get(t); };

        //row identities
        for (const r of ctx.records) {
            const pkAddr = r.cells.get(schema.table(r.table).pk);
            if (r.pk.kind === "token") {
                const map = tableMap(ctx.tokens, r.table);
                if (map.has(r.pk.value)) {
                    report.error(3, "duplicate_token", `${r.pk.value} names two new rows in ${r.sheet} (rows ${map.get(r.pk.value).row} and ${r.row}).`, { sheet: r.sheet, cell: pkAddr });
                }
                else map.set(r.pk.value, r);
            }
            else if (r.pk.kind === "id") {
                const map = tableMap(ctx.byId, r.table);
                if (map.has(r.pk.value)) {
                    report.error(3, "duplicate_id", `ID ${r.pk.value} appears twice in ${r.sheet} (rows ${map.get(r.pk.value).row} and ${r.row}).`, { sheet: r.sheet, cell: pkAddr });
                }
                else map.set(r.pk.value, r);
            }
            if (r.action === "delete" && r.pk.kind !== "id") {
                report.error(3, "delete_new_row", `Row ${r.row} is marked delete but is a new row; just remove it instead.`, { sheet: r.sheet, cell: pkAddr });
            }
        }

        //existing ids must belong to this bundle (§10 stage 3)
        const idsByTable = new Map();
        for (const [table, map] of ctx.byId) idsByTable.set(table, [...map.keys()]);
        ctx.live = new Map(); //table -> Map(id -> live row)
        for (const [tableName, ids] of idsByTable) {
            const table = schema.table(tableName);
            const rows = await fetchRows(client, schema, table, `t.${quoteIdent(table.pk)} = any($1::${arrayType(schema, table, table.pk)})`, [ids]);
            ctx.live.set(tableName, new Map(rows.map(row => [row[table.pk], row])));
        }
        for (const r of ctx.records) {
            if (r.pk.kind !== "id") continue;
            const inBaseline = ctx.baseline.has(`${r.table}:${r.pk.value}`);
            const exists = ctx.live.get(r.table).has(r.pk.value);
            r.inBaseline = inBaseline;
            if (r.binding.role === "owned" && !inBaseline) {
                report.error(3, exists ? "row_not_in_bundle" : "unknown_id",
                    exists
                        ? `ID ${r.pk.value} exists in ${r.table} but does not belong to this export. Rows cannot be copied in from another workbook; new rows need an empty ID or a NEW- name.`
                        : `There is no row ${r.pk.value} in ${r.table}. New rows need an empty ID or a NEW- name.`,
                    { sheet: r.sheet, cell: r.cells.get(schema.table(r.table).pk) });
            }
            else if (!inBaseline && !exists && r.binding.role !== "owned") {
                report.error(3, "unknown_id", `There is no row ${r.pk.value} in ${r.table}.`, { sheet: r.sheet, cell: r.cells.get(schema.table(r.table).pk) });
            }
        }

        await this._resolveForeignKeys(ctx);
        this._resolveProposedTables(ctx);
        if (report.hasErrors()) return;
        await this._checkDeletes(ctx);
        this._checkNotNull(ctx);
        await this._checkUniques(ctx);
    }

    /**
     * §7 foreign-key resolution: an integer wins; a token must be defined in the
     * target table's sheet; a blank key with a label is resolved by the label,
     * and an unmatched or ambiguous label is an error.
     */
    async _resolveForeignKeys(ctx) {
        const { report, schema, client } = ctx;
        const existence = new Map(); //"table.column" -> Set(values) to check
        const labelLookups = new Map(); //target table -> Set(label)

        for (const r of ctx.records) {
            const table = schema.table(r.table);
            for (const fk of table.fks) {
                if (!r.values.has(fk.column)) continue;
                const value = r.values.get(fk.column);
                const target = schema.table(fk.parent);
                const pkTarget = target.pk === fk.parentColumn;
                if (value === null || value === undefined) {
                    const label = r.labels.get(fk.column);
                    if (label && pkTarget) {
                        if (!labelLookups.has(fk.parent)) labelLookups.set(fk.parent, new Set());
                        labelLookups.get(fk.parent).add(label.text);
                    }
                    continue;
                }
                if (typeof value === "string" && TOKEN.test(value) && pkTarget) continue; //checked below
                const key = `${fk.parent}.${fk.parentColumn}`;
                if (!existence.has(key)) existence.set(key, new Set());
                existence.get(key).add(value);
            }
        }

        const existing = new Map();
        for (const [key, values] of existence) {
            const [tableName, column] = key.split(".");
            const table = schema.table(tableName);
            const res = await client.query(
                `select t.${quoteIdent(column)}::text as v from public.${quoteIdent(tableName)} t
                 where t.${quoteIdent(column)} = any($1::${arrayType(schema, table, column)})`, [[...values]]);
            existing.set(key, new Set(res.rows.map(r => r.v)));
        }

        const labelMatches = new Map(); //target -> Map(label -> [ids])
        for (const [tableName, labels] of labelLookups) {
            const table = schema.table(tableName);
            const res = await client.query(
                `select (${table.labelExpression})::text as label, t.${quoteIdent(table.pk)} as id
                 from public.${quoteIdent(tableName)} t where (${table.labelExpression})::text = any($1::text[])`, [[...labels]]);
            const map = new Map();
            for (const row of res.rows) {
                if (!map.has(row.label)) map.set(row.label, []);
                map.get(row.label).push(Number(row.id));
            }
            labelMatches.set(tableName, map);
        }

        for (const r of ctx.records) {
            const table = schema.table(r.table);
            for (const fk of table.fks) {
                if (!r.values.has(fk.column)) continue;
                const value = r.values.get(fk.column);
                const target = schema.table(fk.parent);
                const where = { sheet: r.sheet, cell: r.cells.get(fk.column) };
                if (value === undefined) continue; //coercion already failed
                if (value === null) {
                    const label = r.labels.get(fk.column);
                    if (!label || target.pk !== fk.parentColumn) continue;
                    const live = (labelMatches.get(fk.parent) || new Map()).get(label.text) || [];
                    const fresh = this._matchNewRowsByLabel(ctx, target, label.text);
                    const count = live.length + fresh.length;
                    const labelWhere = { sheet: r.sheet, cell: label.address };
                    if (count === 0) {
                        report.error(3, "label_not_found", `${label.address}: no ${target.sheet} is labelled "${label.text}". Pick a value from the list, or fill in ${fk.column}.`, labelWhere);
                    }
                    else if (count > 1) {
                        report.error(3, "label_ambiguous",
                            `${label.address}: "${label.text}" matches ${count} ${target.sheet} rows` +
                            (live.length ? ` (IDs ${live.slice(0, 5).join(", ")}${live.length > 5 ? ", …" : ""})` : "") + `. Fill in ${fk.column} instead.`, labelWhere);
                    }
                    else {
                        r.values.set(fk.column, live.length ? live[0] : fresh[0]);
                        r.resolvedByLabel = r.resolvedByLabel || new Set();
                        r.resolvedByLabel.add(fk.column);
                    }
                    continue;
                }
                if (typeof value === "string" && TOKEN.test(value) && target.pk === fk.parentColumn) {
                    const defined = ctx.tokens.get(fk.parent);
                    if (!defined || !defined.has(value)) {
                        report.error(3, "unknown_token", `${where.cell}: ${value} is not defined as a new row on the ${target.sheet} sheet.`, where);
                    }
                    continue;
                }
                //rows pointing at a row marked delete are reported once, per deleted
                //row, by _checkDeletes
                const ok = existing.get(`${fk.parent}.${fk.parentColumn}`);
                if (!ok || !ok.has(String(value))) {
                    report.error(3, "fk_not_found", `${where.cell}: ${fk.column} = ${value}, but there is no such ${target.sheet} row.`, where);
                }
            }
        }
    }

    /** New rows of a table whose label is a plain column, matched by that column. */
    _matchNewRowsByLabel(ctx, table, text) {
        const m = /^t\."([^"]+)"$/.exec(table.labelExpression);
        if (!m) return [];
        const tokens = ctx.tokens.get(table.name);
        if (!tokens) return [];
        return [...tokens.values()].filter(r => r.values.get(m[1]) === text).map(r => r.pk.value);
    }

    _resolveProposedTables(ctx) {
        const { report } = ctx;
        for (const p of ctx.schemaProposals) {
            if (p.type !== "table") continue;
            for (const a of p.attachments) {
                for (const row of p.rows) {
                    const v = row.values[a.column];
                    const cell = `${p.ws.getColumn(a.index).letter}${row.row}`;
                    if (v === undefined || v === null) {
                        report.error(3, "proposed_row_unattached", `${cell}: every row of a proposed table must say which ${a.table} it belongs to.`, { sheet: p.sheet, cell });
                        continue;
                    }
                    const ok = typeof v === "string" && TOKEN.test(v)
                        ? (ctx.tokens.get(a.table) || new Map()).has(v)
                        : ctx.baseline.has(`${a.table}:${v}`);
                    if (!ok) {
                        report.error(3, "proposed_row_unattached", `${cell}: ${v} is not a ${a.table} row in this workbook.`, { sheet: p.sheet, cell });
                    }
                }
            }
        }
    }

    /**
     * §7: no cascades. A delete is refused while anything that is not also
     * being deleted still points at the row, in the workbook or in the database,
     * and a shared row cannot be deleted at all.
     */
    async _checkDeletes(ctx) {
        const { report, schema, client } = ctx;
        const deletes = ctx.records.filter(r => r.action === "delete" && r.pk.kind === "id" && r.binding.role === "owned");
        const byTable = new Map();
        for (const r of deletes) {
            const baseline = ctx.baseline.get(`${r.table}:${r.pk.value}`);
            if (baseline && baseline.shared) {
                report.error(3, "delete_shared_row",
                    `${r.sheet} ${r.pk.value} is also part of another site; it cannot be deleted from here.`,
                    { sheet: r.sheet, cell: r.cells.get(schema.table(r.table).pk) });
                continue;
            }
            if (!byTable.has(r.table)) byTable.set(r.table, []);
            byTable.get(r.table).push(r);
        }

        const deleted = (tableName, id) => {
            const rec = (ctx.byId.get(tableName) || new Map()).get(id);
            return rec && rec.action === "delete";
        };

        for (const [tableName, recs] of byTable) {
            const table = schema.table(tableName);
            const ids = recs.map(r => r.pk.value);
            const blockers = new Map(); //id -> [descriptions]
            for (const fk of table.incoming) {
                if (fk.parentColumn !== table.pk) continue;
                const child = schema.table(fk.table);
                const res = await client.query(
                    `select t.${quoteIdent(child.pk)} as child_id, t.${quoteIdent(fk.column)} as parent_id
                     from public.${quoteIdent(child.name)} t where t.${quoteIdent(fk.column)} = any($1::${arrayType(schema, child, fk.column)})`,
                    [ids]);
                for (const row of res.rows) {
                    const childId = Number(row.child_id);
                    const parentId = Number(row.parent_id);
                    const inWorkbook = (ctx.byId.get(child.name) || new Map()).get(childId);
                    if (inWorkbook) {
                        if (inWorkbook.action === "delete") continue;
                        const now = inWorkbook.values.get(fk.column);
                        if (now !== undefined && now !== parentId) continue; //re-pointed elsewhere
                    }
                    if (deleted(child.name, childId)) continue;
                    if (!blockers.has(parentId)) blockers.set(parentId, new Map());
                    const m = blockers.get(parentId);
                    m.set(child.sheet, (m.get(child.sheet) || 0) + 1);
                }
            }
            //new rows in the workbook pointing at a row being deleted
            for (const r of ctx.records) {
                if (r.action === "delete") continue;
                for (const fk of schema.table(r.table).fks) {
                    if (fk.parent !== tableName || fk.parentColumn !== table.pk) continue;
                    const v = r.values.get(fk.column);
                    if (r.pk.kind !== "id" && ids.includes(v)) {
                        if (!blockers.has(v)) blockers.set(v, new Map());
                        const m = blockers.get(v);
                        m.set(`${r.sheet} (new rows)`, (m.get(`${r.sheet} (new rows)`) || 0) + 1);
                    }
                }
            }
            for (const r of recs) {
                const b = blockers.get(r.pk.value);
                if (!b) continue;
                const list = [...b].map(([sheet, n]) => `${n} on ${sheet}`).join(", ");
                report.error(3, "delete_still_referenced",
                    `${r.sheet} ${r.pk.value} cannot be deleted while other rows point at it: ${list}. Mark those delete too, or point them elsewhere.`,
                    { sheet: r.sheet, cell: r.cells.get(table.pk) });
            }
        }
    }

    _checkNotNull(ctx) {
        const { report, schema } = ctx;
        for (const r of ctx.records) {
            if (r.action === "delete" || r.binding.role !== "owned") continue;
            const table = schema.table(r.table);
            for (const column of schema.exportedColumns(table)) {
                if (column.nullable || column.name === table.pk || column.system) continue;
                const present = r.values.has(column.name);
                const value = r.values.get(column.name);
                if (r.pk.kind === "id") {
                    if (present && value === null) {
                        report.error(3, "required_value", `${column.name} cannot be empty.`, { sheet: r.sheet, cell: r.cells.get(column.name) });
                    }
                }
                else if ((value === null || !present) && !column.hasDefault) {
                    report.error(3, "required_value",
                        `A new ${table.sheet} row needs ${column.name}.`,
                        { sheet: r.sheet, cell: r.cells.get(column.name) || `row ${r.row}` });
                }
            }
        }
    }

    /**
     * Unique constraints over the rows this workbook inserts or changes: against
     * each other, and against live rows that are not themselves being changed.
     */
    async _checkUniques(ctx) {
        const { report, schema, client } = ctx;
        const byTable = new Map();
        for (const r of ctx.records) {
            if (r.action === "delete" || r.binding.role !== "owned") continue;
            if (!byTable.has(r.table)) byTable.set(r.table, []);
            byTable.get(r.table).push(r);
        }
        for (const [tableName, recs] of byTable) {
            const table = schema.table(tableName);
            const keyColumns = this._keyColumns(schema, table);
            for (const u of table.uniques) {
                const tuples = new Map(); //key -> record
                const candidates = [];
                for (const r of recs) {
                    const vals = u.columns.map(c => (r.values.has(c) ? r.values.get(c) : (r.pk.kind === "id" ? ctx.live.get(tableName).get(r.pk.value)?.[c] : null)));
                    if (vals.some((v, i) => v === null || v === undefined || (keyColumns.has(u.columns[i]) && typeof v === "string" && TOKEN.test(v)))) continue;
                    if (r.pk.kind === "id") {
                        const live = ctx.live.get(tableName).get(r.pk.value);
                        if (live && u.columns.every((c, i) => live[c] === vals[i])) continue; //unchanged
                    }
                    const key = JSON.stringify(vals);
                    if (tuples.has(key)) {
                        report.error(3, "duplicate_unique",
                            `${u.columns.join(", ")} must be unique; rows ${tuples.get(key).row} and ${r.row} have the same value.`,
                            { sheet: r.sheet, cell: r.cells.get(u.columns[0]) });
                        continue;
                    }
                    tuples.set(key, r);
                    candidates.push({ r, vals });
                }
                if (!candidates.length) continue;
                const params = u.columns.map((c, i) => candidates.map(x => String(x.vals[i])));
                const res = await client.query(
                    `select t.${quoteIdent(table.pk)} as id, ${u.columns.map(c => `t.${quoteIdent(c)}::text as ${quoteIdent(c)}`).join(", ")}
                     from public.${quoteIdent(tableName)} t
                     where (${u.columns.map(c => `t.${quoteIdent(c)}::text`).join(", ")}) in
                           (select * from unnest(${u.columns.map((c, i) => `$${i + 1}::text[]`).join(", ")}))`, params);
                for (const row of res.rows) {
                    const liveId = Number(row.id);
                    const changing = (ctx.byId.get(tableName) || new Map()).get(liveId);
                    const key = JSON.stringify(u.columns.map(c => row[c]));
                    const hit = candidates.find(x => JSON.stringify(x.vals.map(String)) === key);
                    if (!hit || (hit.r.pk.kind === "id" && hit.r.pk.value === liveId)) continue;
                    if (changing && (changing.action === "delete" ||
                        u.columns.some(c => changing.values.has(c) && String(changing.values.get(c)) !== row[c]))) continue;
                    report.error(3, "duplicate_unique",
                        `${u.columns.join(", ")} must be unique, and ${table.sheet} ${liveId} already has ${u.columns.map(c => row[c]).join(", ")}.` +
                        (u.columns.some(c => /uuid$/.test(c)) ? " A copied row keeps its UUID; clear it and one is generated." : ""),
                        { sheet: hit.r.sheet, cell: hit.r.cells.get(u.columns[0]) });
                }
            }
        }
    }

    //========================================================= stage 4: diff

    async _diff(ctx) {
        const { report, schema } = ctx;
        const cs = {
            inserts: [], updates: [], deletes: [], conflicts: [],
            proposals: [], blocked: [], shared: [], unchanged: 0,
        };
        ctx.changeSet = cs;

        for (const r of ctx.records) {
            const table = schema.table(r.table);
            const role = r.binding.role;
            const where = { sheet: r.sheet, cell: r.cells.get(table.pk) };

            if (r.pk.kind !== "id") {
                const entry = { table: r.table, sheet: r.sheet, row: r.row, token: r.pk.kind === "token" ? r.pk.value : null, values: this._valuesOf(r), deferred: this._proposedOf(r) };
                if (role === "owned") cs.inserts.push(entry);
                else if (role === "reference") cs.proposals.push({ kind: "reference", op: "insert", ...entry });
                else report.warning(4, "readonly_edit", `Row ${r.row} adds to a read-only legacy table; it was ignored.`, where);
                continue;
            }

            const live = ctx.live.get(r.table).get(r.pk.value);
            const baseline = ctx.baseline.get(`${r.table}:${r.pk.value}`);
            const baseKeys = r.binding.baseKeys.length ? r.binding.baseKeys : [...r.binding.data.keys()];

            if (!live) {
                if (r.action === "delete") { cs.unchanged++; continue; } //rule 5: already applied
                cs.conflicts.push({ table: r.table, sheet: r.sheet, row: r.row, id: r.pk.value, reason: "deleted_since_export",
                    message: `${table.sheet} ${r.pk.value} was deleted from the database after this workbook was exported.` });
                continue;
            }

            const hashW = rowHash(baseKeys.map(k => r.values.get(k)));
            const hashL = rowHash(baseKeys.map(k => live[k]));
            const B = baseline ? baseline.hash : null;
            const diffs = [...r.binding.data.keys()]
                .filter(k => canonicalValue(r.values.get(k)) !== canonicalValue(live[k]))
                .map(k => ({ column: k, before: live[k], after: r.values.get(k) }));

            if (r.action === "delete") {
                if (role !== "owned") {
                    if (role === "reference") cs.proposals.push({ kind: "reference", op: "delete", table: r.table, sheet: r.sheet, row: r.row, id: r.pk.value });
                    else report.warning(4, "readonly_edit", `Row ${r.row} of a read-only legacy table is marked delete; it was ignored.`, where);
                    continue;
                }
                if (B !== null && hashL !== B) {
                    cs.conflicts.push({ table: r.table, sheet: r.sheet, row: r.row, id: r.pk.value, reason: "changed_since_export",
                        message: `${table.sheet} ${r.pk.value} is marked delete, but it was changed in the database after export.`,
                        live: this._plain(live) });
                    continue;
                }
                cs.deletes.push({ table: r.table, sheet: r.sheet, row: r.row, id: r.pk.value, before: this._plain(live) });
                continue;
            }

            let outcome;
            if (diffs.length === 0) outcome = "none";            //rule 1
            else if (B === null) outcome = "update";             //row not in baseline (reference rows only)
            else if (hashW === B && hashL !== B) {
                //rule 2, for the exported columns; columns added since export still count
                const extra = diffs.filter(d => !baseKeys.includes(d.column));
                outcome = extra.length ? "update-extra" : "none";
            }
            else if (hashW !== B && hashL === B) outcome = "update"; //rule 3
            else if (hashW === B && hashL === B) outcome = "update"; //only columns outside the baseline differ
            else outcome = "conflict";                           //rule 4

            const deferred = this._proposedOf(r);
            if (outcome === "none" && !Object.keys(deferred).length) { cs.unchanged++; continue; }

            //rule 2 wins over proposed columns: a row left alone here writes nothing,
            //even when it carries values in a proposed column
            const fields = outcome === "none" ? []
                : outcome === "update-extra" ? diffs.filter(d => !baseKeys.includes(d.column))
                : diffs;
            if (outcome === "conflict") {
                cs.conflicts.push({ table: r.table, sheet: r.sheet, row: r.row, id: r.pk.value, reason: "changed_on_both_sides",
                    message: `${table.sheet} ${r.pk.value} was edited in this workbook and also changed in the database after export.`,
                    fields: diffs.map(d => ({ column: d.column, workbook: d.after, live: d.before })) });
                continue;
            }
            if (role === "reference") {
                if (fields.length) cs.proposals.push({ kind: "reference", op: "update", table: r.table, sheet: r.sheet, row: r.row, id: r.pk.value, fields });
                else cs.unchanged++;
                continue;
            }
            if (role === "reference-deprecated") {
                if (fields.length) report.warning(4, "readonly_edit", `Changes to ${table.sheet} ${r.pk.value} (a read-only legacy table) were ignored.`, where);
                cs.unchanged++;
                continue;
            }
            const update = { table: r.table, sheet: r.sheet, row: r.row, id: r.pk.value, fields, deferred, before: this._plain(live) };
            if (!fields.length) {
                //only values in proposed columns: nothing to apply now; reported with the proposal
                cs.unchanged++;
                if (Object.keys(deferred).length) cs.blocked.push({ ...update, reason: "proposed_columns_only" });
                continue;
            }
            cs.updates.push(update);
            if (baseline && baseline.shared) cs.shared.push({ table: r.table, id: r.pk.value });
        }

        this._collectProposals(ctx);
        this._propagateBlocking(ctx);
        await this._sharedSites(ctx);

        report.change_set = cs;
        report.summary = {
            inserts: cs.inserts.length,
            updates: cs.updates.length,
            deletes: cs.deletes.length,
            conflicts: cs.conflicts.length,
            proposals: cs.proposals.length,
            blocked: cs.blocked.length,
            unchanged_rows: cs.unchanged,
            empty: cs.inserts.length + cs.updates.length + cs.deletes.length + cs.conflicts.length + cs.proposals.length === 0,
        };
    }

    _valuesOf(r) {
        const out = {};
        for (const [k, v] of r.values) {
            if (v !== null && v !== undefined) out[k] = v;
        }
        return out;
    }

    _proposedOf(r) {
        const out = {};
        for (const [k, v] of r.proposed) out[k] = v.value;
        return out;
    }

    _plain(row) {
        return { ...row };
    }

    /** Schema proposals as report entries: name, source, inferred type, samples. */
    _collectProposals(ctx) {
        const cs = ctx.changeSet;
        for (const p of ctx.schemaProposals) {
            if (p.type === "column") {
                const values = ctx.records.filter(r => r.sheet === p.sheet && r.proposed.has(p.column)).map(r => r.proposed.get(p.column).value);
                cs.proposals.push({
                    kind: "schema", type: "column", table: p.table, sheet: p.sheet, column: p.column,
                    inferred_type: inferType(values), rows_with_values: values.length, sample_values: values.slice(0, 10),
                });
            }
            else {
                const columns = p.headers.filter(h => h.key !== "_action").map(h => {
                    const values = p.rows.map(r => r.values[h.key]).filter(v => v !== undefined);
                    return { name: h.key, inferred_type: h.key === p.pk ? "integer (primary key)" : inferType(values), sample_values: values.slice(0, 5) };
                });
                cs.proposals.push({
                    kind: "schema", type: "table", sheet: p.sheet, proposed_table: `tbl_${p.sheet}`,
                    primary_key: p.pk, attaches_to: p.attachments.map(a => ({ column: a.column, table: a.table })),
                    columns, row_count: p.rows.length, rows: p.rows.map(r => r.values),
                });
            }
        }
    }

    /**
     * §10: an entry depending on a proposal is blocked, and so is anything that
     * depends on a blocked entry.
     */
    _propagateBlocking(ctx) {
        const { schema } = ctx;
        const cs = ctx.changeSet;
        const blocked = new Set();
        for (const p of cs.proposals) {
            if (p.kind === "reference" && p.op === "insert" && p.token) blocked.add(`${p.table}:${p.token}`);
        }
        const dependsOnBlocked = entry => schema.table(entry.table).fks.some(fk => {
            const v = entry.values ? entry.values[fk.column] : (entry.fields || []).find(f => f.column === fk.column)?.after;
            return typeof v === "string" && TOKEN.test(v) && blocked.has(`${fk.parent}:${v}`);
        });
        let changed = true;
        while (changed) {
            changed = false;
            for (let i = cs.inserts.length - 1; i >= 0; i--) {
                const e = cs.inserts[i];
                if (!dependsOnBlocked(e)) continue;
                cs.inserts.splice(i, 1);
                cs.blocked.push({ ...e, reason: "depends_on_proposal" });
                if (e.token) blocked.add(`${e.table}:${e.token}`);
                changed = true;
            }
        }
        for (let i = cs.updates.length - 1; i >= 0; i--) {
            if (!dependsOnBlocked(cs.updates[i])) continue;
            cs.blocked.push({ ...cs.updates[i], reason: "depends_on_proposal" });
            cs.updates.splice(i, 1);
        }
    }

    /** For shared updates: which other sites the change also affects. */
    async _sharedSites(ctx) {
        const { client } = ctx;
        const cs = ctx.changeSet;
        for (const s of cs.shared) {
            const datasetId = s.table === "tbl_datasets" ? s.id
                : (cs.updates.find(u => u.table === s.table && u.id === s.id)?.before || {}).dataset_id;
            if (!datasetId) { s.other_site_ids = null; continue; }
            const res = await client.query(`
                select distinct sg.site_id from public.tbl_analysis_entities ae
                join public.tbl_physical_samples ps using (physical_sample_id)
                join public.tbl_sample_groups sg using (sample_group_id)
                where ae.dataset_id = $1 and not (sg.site_id = any($2::int4[])) order by 1`, [datasetId, ctx.bundleSiteIds]);
            s.other_site_ids = res.rows.map(r => r.site_id);
        }
    }
}

function inferType(values) {
    const present = values.filter(v => v !== null && v !== undefined && v !== "");
    if (!present.length) return "unknown (no values)";
    if (present.every(v => typeof v === "boolean")) return "boolean";
    if (present.every(v => typeof v === "number" && Number.isInteger(v))) return "integer";
    if (present.every(v => typeof v === "number")) return "numeric";
    if (present.every(v => typeof v === "string" && ISO_DATE.test(v))) return "date";
    const longest = Math.max(...present.map(v => String(v).length));
    return longest > 255 ? "text" : `varchar (longest value ${longest})`;
}
