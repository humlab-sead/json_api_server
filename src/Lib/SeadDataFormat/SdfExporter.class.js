import crypto from "crypto";
import SdfSchema from "./SdfSchema.class.js";
import { fetchRows, arrayType } from "./SdfRows.js";
import { SDF_VERSION, SdfError, DEFAULT_MAX_CELLS, quoteIdent, rowHash, machineChecksum } from "./SdfCommon.js";

/**
 * Builds the tabular structure of an SDF workbook (spec §2, the Exporter role).
 *
 * The output is data, not a file: an ordered list of sheets, each with its
 * column descriptors and rows, plus the three machine sheets. SdfRenderer turns
 * it into .xlsx. Keeping the two apart is what lets the exporter be tested
 * without opening a spreadsheet.
 *
 * Everything is read inside one REPEATABLE READ, READ ONLY transaction, so the
 * rows, the labels, the baseline hashes and the recorded database state all
 * describe the same snapshot.
 *
 * Row selection is set-based and schema-driven (§3): one query per owned table,
 * keyed by the ids selected from its parents. There is no method allowlist and
 * nothing here knows what any table means; a self-audit then recounts every
 * table by an independent route and refuses to export on any mismatch.
 */
//§6: a shared list shipped in full feeds a dropdown. One far larger than any today
//(the largest has a few hundred rows) belongs in USED_ONLY_REFERENCES instead.
const MAX_FULL_LIST_ROWS = 2000;

export default class SdfExporter {

    constructor(app) {
        this.app = app;
        this.maxCells = parseInt(process.env.SDF_MAX_CELLS) || DEFAULT_MAX_CELLS;
    }

    /**
     * @param {number[]} siteIds
     * @param {object} opts
     * @param {string|null} opts.exportedBy authenticated user, if any
     */
    async export(siteIds, opts = {}) {
        const client = await this.app.pgPool.connect();
        try {
            await client.query("begin isolation level repeatable read read only");

            const schema = await SdfSchema.load(client);
            const sites = await this._requireSites(client, siteIds);

            const owned = await this._selectOwnedRows(client, schema, siteIds);
            await this._audit(client, schema, siteIds, owned);
            const referenced = await this._selectReferencedRows(client, schema, owned);
            const labels = await this._loadLabels(client, schema, owned, referenced);

            const sheets = this._buildSheets(schema, owned, referenced, labels);
            this._requireSize(sheets, siteIds);
            const machine = this._buildMachineSheets(sheets);
            const meta = await this._buildMeta(client, sites, opts.exportedBy || null);
            meta.push(["checksum", machineChecksum([["key", "value"], ...meta], machine.columns, machine.baseline)]);

            await client.query("commit");

            return {
                sdf_version: SDF_VERSION,
                meta,
                sheets,
                machine: {
                    columns: machine.columns,
                    baseline: machine.baseline,
                },
                lists: this._buildLists(schema, sheets, referenced, labels),
                missingOwnedTables: this._missingOwnedTables(schema, owned),
            };
        }
        catch (err) {
            await client.query("rollback").catch(() => {});
            throw err;
        }
        finally {
            client.release();
        }
    }

    _requireSize(sheets, siteIds) {
        const cells = sheets.reduce((n, s) => n + s.rows.length * s.columns.length, 0);
        if (cells > this.maxCells) {
            throw new SdfError("export_too_large",
                `${siteIds.length === 1 ? "This site makes" : `These ${siteIds.length} sites make`} a workbook of ${cells.toLocaleString("en")} cells, more than the ${this.maxCells.toLocaleString("en")} allowed in one export. Export fewer sites at a time.`,
                { cells, maxCells: this.maxCells }, 413);
        }
    }

    async _requireSites(client, siteIds) {
        const res = await client.query(
            "select site_id, site_name from public.tbl_sites where site_id = any($1::int4[]) order by site_id",
            [siteIds]);
        const found = new Set(res.rows.map(r => r.site_id));
        const missing = siteIds.filter(id => !found.has(id));
        if (missing.length) {
            throw new SdfError("site_not_found", `No site with id ${missing.join(", ")}.`, { missing }, 404);
        }
        return res.rows;
    }

    //------------------------------------------------------------------ rows

    /**
     * §3 row selection. Returns Map(table name -> { rows, shared:Set(pk) }).
     *
     * A closure table's rows are those whose ownership foreign keys point at a
     * selected parent row. A table reached against the walk (datasets, features)
     * takes the rows its owned child references. A row is shared when it is also
     * reachable from a site outside the bundle; only the reverse-reached tables
     * can make that happen, and it propagates to what hangs off them.
     */
    async _selectOwnedRows(client, schema, siteIds) {
        const selected = new Map();
        const valuesOf = (tableName, column) => {
            const entry = selected.get(tableName);
            if (!entry) return [];
            const set = new Set();
            for (const row of entry.rows) {
                if (row[column] !== null) set.add(row[column]);
            }
            return [...set];
        };
        const sharedValuesOf = (tableName, column) => {
            const entry = selected.get(tableName);
            const set = new Set();
            if (!entry || entry.shared.size === 0) return set;
            const pk = schema.table(tableName).pk;
            for (const row of entry.rows) {
                if (entry.shared.has(row[pk]) && row[column] !== null) set.add(row[column]);
            }
            return set;
        };

        for (const name of schema.fetchOrder) {
            const entry = schema.owned.get(name);
            const table = entry.table;
            if (entry.deprecated === "excluded") continue;

            let rows;
            let shared = new Set();
            if (entry.reach === "root") {
                rows = await fetchRows(client, schema, table, `t.${quoteIdent(table.pk)} = any($1::int4[])`, [siteIds]);
            }
            else if (entry.reach === "reverse") {
                const via = entry.via;
                const ids = valuesOf(via.table, via.column);
                rows = ids.length
                    ? await fetchRows(client, schema, table,
                        `t.${quoteIdent(via.parentColumn)} = any($1::${arrayType(schema, table, via.parentColumn)})`, [ids])
                    : [];
                if (rows.length) {
                    const viaTable = schema.table(via.table);
                    const inBundle = selected.get(via.table).rows.map(r => r[viaTable.pk]);
                    const res = await client.query(
                        `select distinct ${quoteIdent(via.column)} as v from public.${quoteIdent(via.table)}
                         where ${quoteIdent(via.column)} = any($1::${arrayType(schema, viaTable, via.column)})
                           and not (${quoteIdent(viaTable.pk)} = any($2::${arrayType(schema, viaTable, viaTable.pk)}))`,
                        [ids, inBundle]);
                    const sharedValues = new Set(res.rows.map(r => Number(r.v)));
                    for (const row of rows) {
                        if (sharedValues.has(row[via.parentColumn])) shared.add(row[table.pk]);
                    }
                }
            }
            else {
                const clauses = [];
                const params = [];
                for (const fk of entry.edges) {
                    const ids = valuesOf(fk.parent, fk.parentColumn);
                    if (!ids.length) continue;
                    params.push(ids);
                    clauses.push(`t.${quoteIdent(fk.column)} = any($${params.length}::${arrayType(schema, table, fk.column)})`);
                }
                rows = clauses.length ? await fetchRows(client, schema, table, clauses.join(" or "), params) : [];
                for (const fk of entry.edges) {
                    const sharedParents = sharedValuesOf(fk.parent, fk.parentColumn);
                    if (!sharedParents.size) continue;
                    for (const row of rows) {
                        if (sharedParents.has(row[fk.column])) shared.add(row[table.pk]);
                    }
                }
            }
            selected.set(name, { rows, shared });
        }
        return selected;
    }

    /**
     * The self-audit (implementation plan, Phase 1). Every owned table is
     * recounted with a single SQL predicate that walks the same ownership rules
     * back to tbl_sites as nested subqueries - an independent route from the
     * id lists above. Any difference means rows would silently go missing, so
     * the export is refused.
     */
    async _audit(client, schema, siteIds, owned) {
        let alias = 0;
        const predicate = (name, a) => {
            const entry = schema.owned.get(name);
            const table = entry.table;
            if (entry.reach === "root") {
                return `${a}.${quoteIdent(table.pk)} = any($1::int4[])`;
            }
            if (entry.reach === "reverse") {
                const via = entry.via;
                const b = `a${++alias}`;
                return `${a}.${quoteIdent(via.parentColumn)} in (select ${b}.${quoteIdent(via.column)}
                        from public.${quoteIdent(via.table)} ${b} where ${predicate(via.table, b)})`;
            }
            return entry.edges.map(fk => {
                const b = `a${++alias}`;
                return `${a}.${quoteIdent(fk.column)} in (select ${b}.${quoteIdent(fk.parentColumn)}
                        from public.${quoteIdent(fk.parent)} ${b} where ${predicate(fk.parent, b)})`;
            }).join(" or ") || "false";
        };

        const mismatches = [];
        for (const [name, { rows }] of owned) {
            const res = await client.query(
                `select count(*)::int as n from public.${quoteIdent(name)} a0 where ${predicate(name, "a0")}`, [siteIds]);
            if (res.rows[0].n !== rows.length) {
                mismatches.push({ table: name, exported: rows.length, expected: res.rows[0].n });
            }
        }
        if (mismatches.length) {
            throw new SdfError("audit_mismatch",
                "The export would not contain every row belonging to these sites, so it was refused.",
                { mismatches }, 500);
        }
    }

    /**
     * §6: referenced rows. Full tables are shipped whole; used-only tables ship
     * the rows some exported row points at.
     */
    async _selectReferencedRows(client, schema, owned) {
        const referenced = new Map();
        for (const name of schema.referencedOrder) {
            const { table, mode } = schema.referenced.get(name);
            let rows;
            if (mode === "full") {
                rows = await fetchRows(client, schema, table, "true", []);
                if (rows.length > MAX_FULL_LIST_ROWS) {
                    throw new SdfError("unsupported_schema",
                        `${name} has ${rows.length} rows, too many to ship in full as a dropdown list (at most ${MAX_FULL_LIST_ROWS}). It needs to be added to the used-only shared lists (§6).`,
                        { table: name, rows: rows.length }, 500);
                }
            }
            else {
                const byColumn = new Map();
                for (const [ownedName, { rows: ownedRows }] of owned) {
                    for (const fk of schema.table(ownedName).fks) {
                        if (fk.parent !== name) continue;
                        const set = byColumn.get(fk.parentColumn) || new Set();
                        for (const row of ownedRows) {
                            if (row[fk.column] !== null) set.add(row[fk.column]);
                        }
                        byColumn.set(fk.parentColumn, set);
                    }
                }
                const clauses = [];
                const params = [];
                for (const [column, set] of byColumn) {
                    if (!set.size) continue;
                    params.push([...set]);
                    clauses.push(`t.${quoteIdent(column)} = any($${params.length}::${arrayType(schema, table, column)})`);
                }
                rows = clauses.length ? await fetchRows(client, schema, table, clauses.join(" or "), params) : [];
            }
            referenced.set(name, { rows, mode });
        }
        return referenced;
    }

    /**
     * §5: the display label of every row a foreign key in the workbook points
     * at, plus every row of each fully shipped reference table (those feed the
     * dropdowns), with colliding labels suffixed by their key. One query per
     * target table and column.
     * Returns Map("table.column" -> Map(String(key) -> label)).
     */
    async _loadLabels(client, schema, owned, referenced) {
        const wanted = new Map(); //"table.column" -> { table, column, values:Set }
        const want = (tableName, column, value) => {
            const key = `${tableName}.${column}`;
            if (!wanted.has(key)) wanted.set(key, { tableName, column, values: new Set() });
            if (value !== null && value !== undefined) wanted.get(key).values.add(value);
        };
        const collect = (tableName, rows) => {
            for (const fk of schema.table(tableName).fks) {
                want(fk.parent, fk.parentColumn, null);
                for (const row of rows) want(fk.parent, fk.parentColumn, row[fk.column]);
            }
        };
        for (const [name, { rows }] of owned) collect(name, rows);
        for (const [name, { rows }] of referenced) collect(name, rows);
        for (const [name, { rows, mode }] of referenced) {
            if (mode !== "full") continue;
            const pk = schema.table(name).pk;
            for (const row of rows) want(name, pk, row[pk]);
        }

        const labels = new Map();
        for (const [key, { tableName, column, values }] of wanted) {
            const map = new Map();
            labels.set(key, map);
            if (!values.size) continue;
            const table = schema.table(tableName);
            const res = await client.query(
                `select t.${quoteIdent(column)}::text as k, (${table.labelExpression})::text as label
                 from public.${quoteIdent(tableName)} t
                 where t.${quoteIdent(column)} = any($1::${arrayType(schema, table, column)})`,
                [[...values]]);
            for (const row of res.rows) map.set(row.k, row.label);
            //§5: labels that collide are made unique by their key, "Pollen analysis [118]",
            //so a dropdown never offers a choice that resolves to two rows
            const seen = new Map();
            for (const label of map.values()) seen.set(label, (seen.get(label) || 0) + 1);
            for (const [k, label] of map) {
                if (label !== null && seen.get(label) > 1) map.set(k, `${label} [${k}]`);
            }
        }
        return labels;
    }

    //---------------------------------------------------------------- sheets

    /**
     * §4 / §5: one sheet per table with rows (plus every fully shipped reference
     * table), columns in the order _action, then the table's columns with each
     * foreign key followed by its label.
     */
    _buildSheets(schema, owned, referenced, labels) {
        const sheets = [];
        for (const name of schema.ownedOrder) {
            const entry = schema.owned.get(name);
            const data = owned.get(name);
            if (!data || data.rows.length === 0) continue;
            const role = entry.deprecated === "read-only" ? "reference-deprecated" : "owned";
            sheets.push(this._sheet(schema, entry.table, role, null, data.rows, data.shared, labels));
        }
        for (const name of schema.referencedOrder) {
            const { table, mode, deprecated } = schema.referenced.get(name);
            const data = referenced.get(name);
            if (mode === "used-only" && data.rows.length === 0) continue;
            sheets.push(this._sheet(schema, table, deprecated ? "reference-deprecated" : "reference", mode, data.rows, new Set(), labels));
        }
        return sheets;
    }

    _sheet(schema, table, role, referenceMode, rows, shared, labels) {
        const columns = [{
            key: "_action", kind: "action", source: null, pgType: null, valueKind: "text",
            nullable: null, fkTable: null, labelExpression: null, comment: null,
        }];
        const cellFns = [() => null];

        for (const col of schema.exportedColumns(table)) {
            columns.push({
                key: col.name,
                kind: col.system ? "system" : "data",
                source: `public.${table.name}.${col.name}`,
                pgType: col.pgType,
                valueKind: col.valueKind,
                nullable: col.nullable,
                isPk: col.name === table.pk,
                fkTable: col.fk ? `public.${col.fk.parent}.${col.fk.parentColumn}` : null,
                labelExpression: null,
                comment: col.comment,
            });
            cellFns.push(row => row[col.name]);

            if (col.fk) {
                const target = schema.table(col.fk.parent);
                const map = labels.get(`${col.fk.parent}.${col.fk.parentColumn}`) || new Map();
                columns.push({
                    key: `${col.name}:label`,
                    kind: "label",
                    source: `label:public.${col.fk.parent}`,
                    pgType: null,
                    valueKind: "text",
                    nullable: null,
                    fkTable: `public.${col.fk.parent}.${col.fk.parentColumn}`,
                    labelExpression: target.labelExpression,
                    labelTarget: col.fk.parent,
                    comment: `Label of the ${target.sheet} row that ${col.name} points at. For reading, and for choosing a value on a new row; ignored when ${col.name} is filled.`,
                });
                cellFns.push(row => {
                    const v = row[col.name];
                    return v === null ? null : (map.get(String(v)) ?? null);
                });
            }
        }

        const hashIndexes = columns
            .map((c, i) => (c.kind === "data" ? i : -1))
            .filter(i => i >= 0);

        const outRows = [];
        const baseline = [];
        for (const row of rows) {
            const out = cellFns.map(fn => fn(row));
            outRows.push(out);
            baseline.push({
                id: row[table.pk],
                hash: rowHash(hashIndexes.map(i => out[i])),
                shared: shared.has(row[table.pk]),
            });
        }

        return {
            name: table.sheet,
            table: table.name,
            role,
            referenceMode,
            description: table.comment,
            columns,
            rows: outRows,
            baseline,
        };
    }

    /**
     * §9: _sdf_columns and _sdf_baseline.
     */
    _buildMachineSheets(sheets) {
        const columns = [["sheet", "table", "key", "kind", "source", "pg_type", "nullable", "fk_table", "label_expr", "sheet_role"]];
        const baseline = [["table", "id", "hash", "shared"]];
        for (const sheet of sheets) {
            for (const c of sheet.columns) {
                columns.push([
                    sheet.name, sheet.table, c.key, c.kind, c.source, c.pgType,
                    c.nullable === null ? null : c.nullable, c.fkTable, c.labelExpression, sheet.role,
                ]);
            }
            for (const b of sheet.baseline) {
                baseline.push([sheet.table, b.id, b.hash, b.shared]);
            }
        }
        return { columns, baseline };
    }

    /**
     * §9: _sdf_meta. Everything that pins the database state the IDs belong to:
     * the cluster identity, the server version, the latest release tag and the
     * last deployed change of every Sqitch project.
     */
    async _buildMeta(client, sites, exportedBy) {
        const facts = (await client.query(`
            select to_char(now() at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as exported_at,
                   current_database() as database_name,
                   current_setting('server_version') as server_version`)).rows[0];

        let systemIdentifier = null;
        try {
            await client.query("savepoint sdf_meta");
            systemIdentifier = (await client.query(
                "select system_identifier::text as id from pg_control_system()")).rows[0].id;
            await client.query("release savepoint sdf_meta");
        }
        catch (err) {
            await client.query("rollback to savepoint sdf_meta");
        }

        let release = null;
        const projects = [];
        try {
            await client.query("savepoint sdf_sqitch");
            const tag = await client.query("select tag from sqitch.tags order by planned_at desc, committed_at desc limit 1");
            release = tag.rows.length ? tag.rows[0].tag : null;
            const res = await client.query(`
                select p.project,
                       (select c.change from sqitch.changes c where c.project = p.project order by c.committed_at desc limit 1) as change,
                       (select c.change_id from sqitch.changes c where c.project = p.project order by c.committed_at desc limit 1) as change_id,
                       (select t.tag from sqitch.tags t where t.project = p.project order by t.planned_at desc limit 1) as tag
                from sqitch.projects p order by p.project`);
            for (const row of res.rows) projects.push(row);
            await client.query("release savepoint sdf_sqitch");
        }
        catch (err) {
            //no sqitch registry on this database: §9 leaves these blank
            await client.query("rollback to savepoint sdf_sqitch");
        }

        const meta = [
            ["sdf_version", SDF_VERSION],
            ["exporter", `${this.app.appName}-${this.app.appVersion}`],
            ["export_id", crypto.randomUUID()],
            ["exported_at", facts.exported_at],
            ["exported_by", exportedBy],
            ["site_ids", sites.map(s => s.site_id).join(",")],
            ["site_names", sites.map(s => s.site_name).join("\n")],
            ["database_name", facts.database_name],
            ["database_system_identifier", systemIdentifier],
            ["database_server_version", facts.server_version],
            ["database_release", release],
        ];
        for (const p of projects) {
            meta.push([`sqitch:${p.project}`, [p.change, p.change_id, p.tag].filter(Boolean).join(" · ") || null]);
        }
        return meta;
    }

    /**
     * Dropdown sources (§11): for each fully shipped reference sheet, the labels
     * of all its rows in sheet order. Rendered into a hidden sheet the list
     * validations point at.
     */
    _buildLists(schema, sheets, referenced, labels) {
        const lists = [];
        for (const sheet of sheets) {
            if (sheet.role !== "reference" || sheet.referenceMode !== "full") continue;
            const table = schema.table(sheet.table);
            const map = labels.get(`${table.name}.${table.pk}`) || new Map();
            const values = referenced.get(table.name).rows.map(r => map.get(String(r[table.pk])) ?? null);
            lists.push({ table: table.name, sheet: sheet.name, values });
        }
        return lists;
    }

    /**
     * For the user guide (Appendix D §6): the owned tables this workbook has no
     * sheet for, with the columns a curator would need to start one.
     */
    _missingOwnedTables(schema, owned) {
        const missing = [];
        for (const name of schema.ownedOrder) {
            const entry = schema.owned.get(name);
            if (entry.deprecated) continue;
            const data = owned.get(name);
            if (data && data.rows.length) continue;
            missing.push({
                sheet: entry.table.sheet,
                table: name,
                description: entry.table.comment,
                columns: schema.exportedColumns(entry.table).map(c => c.name),
            });
        }
        return missing;
    }
}
