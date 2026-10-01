import crypto from "crypto";
import JSZip from "jszip";
import SdfValidator from "./SdfValidator.class.js";
import { SDF_VERSION, SdfError, quoteIdent } from "./SdfCommon.js";

/**
 * Import stage 5 (spec §10): renders a validated change set as a Sqitch change
 * request for sead_change_control. SDF never applies a change itself; the
 * bundle this produces is reviewed, validated on staging and released like any
 * hand-written change request.
 *
 * The bundle:
 *   <name>/deploy/<name>.sql   guards, then inserts, updates, deletes, sequences, then
 *                              the verify checks, so a deploy that did not land exactly
 *                              as planned rolls itself back
 *   <name>/revert/<name>.sql   the exact inverse, guarded the same way
 *   <name>/verify/<name>.sql   asserts the post-deploy state, inside BEGIN/ROLLBACK
 *   <name>/change.json         name, project, plan note, issue text
 *   <name>/report.json         the full stage 1-4 report
 *   <name>/source.xlsx         the workbook as uploaded
 *   <name>/proposals/*.md      one draft issue per proposal
 *
 * Entries that depend on a proposal are left out and listed in report.json.
 * There is no follow-up workbook: once the proposals are released, the curator
 * exports a fresh workbook and makes those edits there.
 *
 * Everything is computed inside the validator's read-only snapshot, so the
 * before-images the guards check and the ids chosen for new rows describe the
 * same database state as the validation report.
 */

const TOKEN = /^NEW-[A-Za-z0-9_-]{1,32}$/;

//The Sqitch project every SDF change request belongs to. It is listed last in
//sead_change_control's projects.txt, so in a release it deploys after every other
//project's changes, which are the state its guards were computed against.
const SQITCH_PROJECT = "sdf";

//Set on every updated row, to the time the change request was generated: a fixed
//literal, so staging and production end up with the same value.
const UPDATED_COLUMN = "date_updated";

export default class SdfChangeRequest {

    constructor(app) {
        this.app = app;
        this.validator = new SdfValidator(app);
    }

    /**
     * @returns {Promise<{ report, bundle: Buffer|null, name: string|null }>}
     */
    async generate(buffer, opts = {}) {
        const { report, extra } = await this.validator.validate(buffer, {
            withSnapshot: ctx => this._build(ctx, buffer, opts),
        });
        if (!report.ok) return { report, bundle: null, name: null };
        return { report, bundle: extra.bundle, name: extra.name };
    }

    async _build(ctx, sourceBuffer, opts) {
        const { schema, client, meta, report } = ctx;
        const cs = ctx.changeSet;
        const dataChanges = cs.inserts.length + cs.updates.length + cs.deletes.length;
        if (!dataChanges && !cs.proposals.length) {
            throw new SdfError("nothing_to_submit",
                cs.conflicts.length
                    ? "The only changes in this workbook are conflicts with changes made in SEAD since export. Resolve them first."
                    : "This workbook changes nothing, so there is no change request to make.", {}, 422);
        }

        const siteIds = ctx.bundleSiteIds;
        const date = (await client.query(`
            select to_char(now(), 'YYYYMMDD') as d, to_char(now(), 'YYYY-MM-DD') as iso,
                   to_char(now() at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as generated_at`)).rows[0];
        const exportId = meta.get("export_id");
        //the export id keeps two change requests for the same sites on the same day apart
        const exportTag = String(exportId || "").replace(/[^0-9A-Fa-f]/g, "").slice(0, 8).toUpperCase();
        const name = `${date.d}_DML_SDF_${siteIds.length <= 3 ? `SITE_${siteIds.join("_")}` : `SITES_${siteIds.length}`}` +
            (exportTag ? `_${exportTag}` : "");
        const author = opts.author || meta.get("exported_by") || "SDF import";

        const ids = await this._allocateIds(ctx);
        const plan = this._plan(ctx, ids, date.generated_at);
        await this._exactDecimals(ctx, plan);

        const files = {};
        const dir = name;
        const project = SQITCH_PROJECT;
        if (dataChanges) {
            const header = this._header(ctx, { name, project, author, date: date.iso, exportId, plan });
            files[`${dir}/deploy/${name}.sql`] = this._deploy(ctx, plan, header, project, name);
            files[`${dir}/revert/${name}.sql`] = this._revert(ctx, plan, project, name);
            files[`${dir}/verify/${name}.sql`] = this._verify(ctx, plan, project, name);
        }

        const proposalFiles = this._proposalIssues(ctx, name, exportId);
        for (const [path, text] of proposalFiles) files[`${dir}/proposals/${path}`] = text;

        const note = `SDF import of site${siteIds.length === 1 ? "" : "s"} ${siteIds.join(", ")}: ` +
            `${plan.inserts.length} insert${plan.inserts.length === 1 ? "" : "s"}, ${plan.updates.length} update${plan.updates.length === 1 ? "" : "s"}, ` +
            `${plan.deletes.length} delete${plan.deletes.length === 1 ? "" : "s"} (sdf-export:${exportId})`;
        files[`${dir}/change.json`] = JSON.stringify({
            name: dataChanges ? name : null,
            project: dataChanges ? project : null,
            plan_note: dataChanges ? note : null,
            issue: dataChanges ? {
                title: name,
                labels: ["change-request", "data-correction"],
                body: this._issueBody(ctx, plan, name, exportId),
            } : null,
            author,
            export: report.export,
            database_changes_since_export: report.database_changes_since_export,
            counts: { inserts: plan.inserts.length, updates: plan.updates.length, deletes: plan.deletes.length,
                conflicts_left_out: cs.conflicts.length, blocked_left_out: cs.blocked.length, proposals: cs.proposals.length },
            workbook_sha256: crypto.createHash("sha256").update(sourceBuffer).digest("hex"),
            generated_by: `${this.app.appName}-${this.app.appVersion}`,
            sdf_version: SDF_VERSION,
        }, null, 2) + "\n";
        files[`${dir}/report.json`] = JSON.stringify(report, null, 2) + "\n";
        files[`${dir}/source.xlsx`] = sourceBuffer;

        const zip = new JSZip();
        for (const [path, content] of Object.entries(files)) zip.file(path, content);
        const bundle = await zip.generateAsync({ type: "nodebuffer", compression: "DEFLATE", compressionOptions: { level: 6 } });
        return { bundle, name: dataChanges ? name : `${date.d}_SDF_PROPOSALS_${siteIds.join("_")}` };
    }

    //------------------------------------------------------------- planning

    /**
     * §10: explicit ids for new rows, the next free values above the live
     * maximum. The deploy guards assert each is still free, so a change request
     * that has gone stale aborts rather than colliding.
     */
    async _allocateIds(ctx) {
        const { schema, client } = ctx;
        const ids = new Map(); //table -> Map(key -> id); key is the token, or "#<sheet>!<row>"
        const byTable = new Map();
        for (const e of ctx.changeSet.inserts) {
            if (!byTable.has(e.table)) byTable.set(e.table, []);
            byTable.get(e.table).push(e);
        }
        for (const [tableName, entries] of byTable) {
            const table = schema.table(tableName);
            const res = await client.query(`select coalesce(max(${quoteIdent(table.pk)}), 0)::bigint as m from public.${quoteIdent(tableName)}`);
            let next = Number(res.rows[0].m) + 1;
            const map = new Map();
            for (const e of entries) {
                e.id = next++;
                map.set(insertKey(e), e.id);
            }
            ids.set(tableName, map);
        }
        return ids;
    }

    /**
     * Resolves tokens to ids and orders everything by foreign-key dependency:
     * inserts parents first, deletes children first. Every update also sets
     * date_updated to the generation time, where the table has one.
     */
    _plan(ctx, ids, generatedAt) {
        const { schema } = ctx;
        const cs = ctx.changeSet;
        const resolve = (tableName, column, value) => {
            if (typeof value !== "string" || !TOKEN.test(value)) return value;
            //tokens stand only in foreign keys to a primary key (§7); elsewhere NEW-… is text
            const fk = schema.table(tableName).fks.find(f => f.column === column && schema.table(f.parent).pk === f.parentColumn);
            if (!fk) return value;
            const map = ids.get(fk.parent);
            if (!map || !map.has(value)) {
                throw new SdfError("unresolved_token", `${tableName}.${column} = ${value} has no row in this change request.`, {}, 500);
            }
            return map.get(value);
        };

        const inserts = cs.inserts.map(e => {
            const table = schema.table(e.table);
            const values = { [table.pk]: e.id };
            for (const [k, v] of Object.entries(e.values)) {
                if (k === table.pk) continue;
                values[k] = resolve(e.table, k, v);
            }
            return { table: e.table, sheet: e.sheet, row: e.row, token: e.token, id: e.id, values };
        });
        const updates = cs.updates.map(u => {
            const fields = u.fields.map(f => ({ column: f.column, before: f.before, after: resolve(u.table, f.column, f.after) }));
            if (schema.column(schema.table(u.table), UPDATED_COLUMN) && !fields.some(f => f.column === UPDATED_COLUMN)) {
                fields.push({ column: UPDATED_COLUMN, before: u.before[UPDATED_COLUMN] ?? null, after: generatedAt });
            }
            return { table: u.table, sheet: u.sheet, row: u.row, id: u.id, before: u.before, fields };
        });
        const deletes = cs.deletes.map(d => ({ table: d.table, sheet: d.sheet, row: d.row, id: d.id, before: d.before }));

        const order = this._tableOrder(schema, new Set([...inserts, ...updates, ...deletes].map(x => x.table)));
        const rank = t => order.indexOf(t);
        inserts.sort((a, b) => rank(a.table) - rank(b.table) || a.row - b.row);
        updates.sort((a, b) => rank(a.table) - rank(b.table) || a.row - b.row);
        deletes.sort((a, b) => rank(b.table) - rank(a.table) || a.row - b.row);
        return { inserts, updates, deletes, order };
    }

    /**
     * The database's own text for every decimal in a before-image (§8, M15). An
     * unconstrained numeric keeps the scale it was written with (12.50), which
     * the carrier number (12.5) loses; guards compare numerically either way,
     * but a revert must write back exactly what was there.
     */
    async _exactDecimals(ctx, plan) {
        const { schema, client } = ctx;
        const byTable = new Map();
        for (const x of [...plan.updates, ...plan.deletes]) {
            if (!byTable.has(x.table)) byTable.set(x.table, []);
            byTable.get(x.table).push(x);
        }
        for (const [tableName, rows] of byTable) {
            const table = schema.table(tableName);
            const decimals = schema.exportedColumns(table).filter(c => c.valueKind === "decimal").map(c => c.name);
            if (!decimals.length) continue;
            const res = await client.query(
                `select t.${quoteIdent(table.pk)} as id, ${decimals.map(c => `t.${quoteIdent(c)}::text as ${quoteIdent(c)}`).join(", ")}
                 from public.${quoteIdent(tableName)} t where t.${quoteIdent(table.pk)} = any($1::${schema.column(table, table.pk).typname}[])`,
                [rows.map(r => r.id)]);
            const exact = new Map(res.rows.map(r => [Number(r.id), r]));
            for (const r of rows) r.beforeText = exact.get(r.id) || {};
        }
    }

    /** Tables ordered parents before children (Kahn's algorithm over the fks). */
    _tableOrder(schema, tables) {
        const deps = new Map([...tables].map(t => [t, new Set(
            schema.table(t).fks.map(fk => fk.parent).filter(p => p !== t && tables.has(p)))]));
        const order = [];
        while (deps.size) {
            const ready = [...deps].filter(([, d]) => [...d].every(p => order.includes(p))).map(([t]) => t).sort();
            if (!ready.length) { order.push(...[...deps.keys()].sort()); break; }
            for (const t of ready) { order.push(t); deps.delete(t); }
        }
        return order;
    }

    //------------------------------------------------------------------ SQL

    /**
     * A literal cast to the column's base type, without length or scale: an
     * explicit cast to varchar(n) truncates silently, while assigning a base-typed
     * value to the column raises on anything that does not fit.
     */
    _literal(schema, tableName, column, value, exact) {
        if (value === null || value === undefined) return "null";
        const col = schema.column(schema.table(tableName), column);
        const type = col ? col.baseType : "text";
        if (typeof value === "number") {
            //the database's own text, where it holds this very number (scale kept)
            const text = typeof exact === "string" && Number(exact) === value ? exact : String(value);
            return `${value < 0 ? `(${text})` : text}::${type}`;
        }
        if (typeof value === "boolean") return value ? "true" : "false";
        return `${stringLiteral(String(value))}::${type}`;
    }

    /** `col is not distinct from <literal>` for every column of a row image. */
    _matches(schema, tableName, image, columns, exact = {}) {
        return columns.map(c => `${quoteIdent(c)} is not distinct from ${this._literal(schema, tableName, c, image[c], exact[c])}`).join("\n              and ");
    }

    _imageColumns(schema, tableName, image) {
        return schema.exportedColumns(schema.table(tableName)).map(c => c.name).filter(c => c in image);
    }

    /** A dollar-quote tag that does not occur in any value. */
    _tag(plan) {
        const text = JSON.stringify(plan);
        let tag = "sdf";
        while (text.includes(`$${tag}$`)) tag += "_";
        return `$${tag}$`;
    }

    _guardBlock(tag, checks) {
        if (!checks.length) return "";
        return `do ${tag}\nbegin\n${checks.join("\n")}\nend ${tag};\n`;
    }

    _raise(message) {
        return `raise exception '${message.replace(/'/g, "''")}';`;
    }

    _deploy(ctx, plan, header, project, name) {
        const { schema } = ctx;
        const tag = this._tag(plan);
        const pkOf = t => quoteIdent(schema.table(t).pk);
        const out = [`-- Deploy ${project}: ${name}`, "", header, "",
            "set client_encoding = 'UTF8';", "set client_min_messages = warning;", "", "begin;", ""];

        //1. guards: the database must be in the state this change was computed against
        const checks = [];
        for (const u of [...plan.updates, ...plan.deletes]) {
            const cols = this._imageColumns(schema, u.table, u.before);
            checks.push(`    if not exists (select 1 from public.${quoteIdent(u.table)}
            where ${pkOf(u.table)} = ${u.id}
              and ${this._matches(schema, u.table, u.before, cols, u.beforeText)}) then
        ${this._raise(`SDF guard: public.${u.table} ${u.id} has changed since this change request was generated.`)}
    end if;`);
        }
        for (const i of plan.inserts) {
            checks.push(`    if exists (select 1 from public.${quoteIdent(i.table)} where ${pkOf(i.table)} = ${i.id}) then
        ${this._raise(`SDF guard: public.${i.table} ${i.id} already exists; regenerate this change request.`)}
    end if;`);
        }
        if (checks.length) {
            out.push("-- 1. Guards: abort unless every row is as it was when this change request was generated.", this._guardBlock(tag, checks));
        }

        if (plan.inserts.length) {
            out.push("-- 2. Inserts, parents before children.");
            out.push(...this._insertStatements(schema, plan.inserts), "");
        }
        if (plan.updates.length) {
            out.push("-- 3. Updates, changed columns only.");
            for (const u of plan.updates) {
                const sets = u.fields.map(f => `${quoteIdent(f.column)} = ${this._literal(schema, u.table, f.column, f.after)}`).join(", ");
                out.push(`update public.${quoteIdent(u.table)} set ${sets} where ${pkOf(u.table)} = ${u.id};`);
            }
            out.push("");
        }
        if (plan.deletes.length) {
            out.push("-- 4. Deletes, children before parents.");
            for (const d of plan.deletes) {
                out.push(`delete from public.${quoteIdent(d.table)} where ${pkOf(d.table)} = ${d.id};`);
            }
            out.push("");
        }
        const sequenced = [...new Set(plan.inserts.map(i => i.table))];
        if (sequenced.length) {
            out.push("-- 5. Sequences past the new ids.");
            for (const t of sequenced) {
                const pk = schema.table(t).pk;
                out.push(`select setval(pg_get_serial_sequence('public.${t}', '${pk}'), greatest((select max(${quoteIdent(pk)}) from public.${quoteIdent(t)}), 1));`);
            }
            out.push("");
        }
        //the verify checks, inside the deploy transaction: they run however Sqitch is
        //invoked (deploy-staging passes --no-verify)
        out.push("-- 6. Verify: abort, and roll everything back, unless every change landed as planned.",
            this._guardBlock(tag, this._verifyChecks(schema, plan)));
        out.push("commit;", "");
        return out.join("\n");
    }

    _insertStatements(schema, inserts) {
        const out = [];
        let i = 0;
        while (i < inserts.length) {
            //consecutive rows of one table with the same columns share a statement
            const first = inserts[i];
            const cols = Object.keys(first.values);
            const group = [first];
            let j = i + 1;
            while (j < inserts.length && inserts[j].table === first.table &&
                   JSON.stringify(Object.keys(inserts[j].values)) === JSON.stringify(cols)) {
                group.push(inserts[j]);
                j++;
            }
            const rows = group.map(g => `    (${cols.map(c => this._literal(schema, g.table, c, g.values[c], g.exact?.[c])).join(", ")})`);
            out.push(`insert into public.${quoteIdent(first.table)} (${cols.map(quoteIdent).join(", ")}) values\n${rows.join(",\n")};`);
            i = j;
        }
        return out;
    }

    _revert(ctx, plan, project, name) {
        const { schema } = ctx;
        const tag = this._tag(plan);
        const pkOf = t => quoteIdent(schema.table(t).pk);
        const out = [`-- Revert ${project}:${name} from pg`, "",
            "-- Generated by SDF import: the exact inverse of the deploy script, guarded against",
            "-- changes made after deploy.", "",
            "set client_encoding = 'UTF8';", "set client_min_messages = warning;", "", "begin;", ""];

        const checks = [];
        for (const i of plan.inserts) {
            checks.push(`    if not exists (select 1 from public.${quoteIdent(i.table)}
            where ${this._matches(schema, i.table, i.values, Object.keys(i.values))}) then
        ${this._raise(`SDF revert guard: public.${i.table} ${i.id} is not as this change request inserted it.`)}
    end if;`);
        }
        for (const u of plan.updates) {
            const after = Object.fromEntries(u.fields.map(f => [f.column, f.after]));
            checks.push(`    if not exists (select 1 from public.${quoteIdent(u.table)}
            where ${pkOf(u.table)} = ${u.id}
              and ${this._matches(schema, u.table, after, Object.keys(after))}) then
        ${this._raise(`SDF revert guard: public.${u.table} ${u.id} has changed since deploy.`)}
    end if;`);
        }
        for (const d of plan.deletes) {
            checks.push(`    if exists (select 1 from public.${quoteIdent(d.table)} where ${pkOf(d.table)} = ${d.id}) then
        ${this._raise(`SDF revert guard: public.${d.table} ${d.id} exists again; cannot restore it.`)}
    end if;`);
        }
        if (checks.length) out.push("-- Guards.", this._guardBlock(tag, checks));

        if (plan.deletes.length) {
            out.push("-- Restore deleted rows, parents before children.");
            const restore = [...plan.deletes].reverse().map(d => ({ table: d.table, row: d.row, id: d.id, exact: d.beforeText,
                values: Object.fromEntries(this._imageColumns(schema, d.table, d.before).map(c => [c, d.before[c]])) }));
            out.push(...this._insertStatements(schema, restore), "");
        }
        if (plan.updates.length) {
            out.push("-- Restore updated columns.");
            for (const u of plan.updates) {
                const sets = u.fields.map(f => `${quoteIdent(f.column)} = ${this._literal(schema, u.table, f.column, f.before, u.beforeText?.[f.column])}`).join(", ");
                out.push(`update public.${quoteIdent(u.table)} set ${sets} where ${pkOf(u.table)} = ${u.id};`);
            }
            out.push("");
        }
        if (plan.inserts.length) {
            out.push("-- Remove inserted rows, children before parents.");
            for (const i of [...plan.inserts].reverse()) {
                out.push(`delete from public.${quoteIdent(i.table)} where ${pkOf(i.table)} = ${i.id};`);
            }
            out.push("");
        }
        out.push("commit;", "");
        return out.join("\n");
    }

    _verify(ctx, plan, project, name) {
        const tag = this._tag(plan);
        return [`-- Verify ${project}:${name} on pg`, "", "begin;", "", this._guardBlock(tag, this._verifyChecks(ctx.schema, plan)), "rollback;", ""].join("\n");
    }

    /** Assertions that the database holds exactly what the deploy script wrote. */
    _verifyChecks(schema, plan) {
        const pkOf = t => quoteIdent(schema.table(t).pk);
        const checks = [];
        for (const i of plan.inserts) {
            checks.push(`    if not exists (select 1 from public.${quoteIdent(i.table)}
            where ${this._matches(schema, i.table, i.values, Object.keys(i.values))}) then
        ${this._raise(`SDF verify: public.${i.table} ${i.id} is missing or differs.`)}
    end if;`);
        }
        for (const u of plan.updates) {
            const after = Object.fromEntries(u.fields.map(f => [f.column, f.after]));
            checks.push(`    if not exists (select 1 from public.${quoteIdent(u.table)}
            where ${pkOf(u.table)} = ${u.id}
              and ${this._matches(schema, u.table, after, Object.keys(after))}) then
        ${this._raise(`SDF verify: public.${u.table} ${u.id} does not hold the updated values.`)}
    end if;`);
        }
        for (const d of plan.deletes) {
            checks.push(`    if exists (select 1 from public.${quoteIdent(d.table)} where ${pkOf(d.table)} = ${d.id}) then
        ${this._raise(`SDF verify: public.${d.table} ${d.id} still exists.`)}
    end if;`);
        }
        return checks;
    }

    _header(ctx, { name, project, author, date, exportId, plan }) {
        const { meta, report } = ctx;
        const sqitch = [...meta].filter(([k, v]) => k && k.startsWith("sqitch:") && v).map(([k, v]) => `${k.slice(7)}: ${v}`);
        const lines = [
            `Author        ${author}`,
            `Date          ${date}`,
            `Description   SDF import for site(s) ${meta.get("site_ids")}: ${count(plan.inserts.length, "insert")}, ${count(plan.updates.length, "update")}, ${count(plan.deletes.length, "delete")}.`,
            "Issue         ",
            "Prerequisites ",
            "Reviewer      ",
            "Approver      ",
            "Idempotent    No. Guarded: aborts unless every affected row is as it was when this was generated.",
            `Notes         Generated by ${this.app.appName}-${this.app.appVersion} (${SDF_VERSION}) from export`,
            `              sdf-export:${exportId}`,
            `              exported ${meta.get("exported_at")} by ${meta.get("exported_by") || "anonymous"}`,
            `              from ${meta.get("database_name")} (release ${meta.get("database_release") || "unknown"}), sites ${meta.get("site_ids")}.`,
            "              Sqitch state at export:",
            ...sqitch.map(s => `                ${s}`),
            report.database_changes_since_export.length
                ? `              ${report.database_changes_since_export.length} change(s) were deployed between export and generation; see report.json.`
                : "              No changes were deployed between export and generation.",
            ctx.changeSet.conflicts.length ? `              ${count(ctx.changeSet.conflicts.length, "conflicting row")} left out; see report.json.` : null,
            ctx.changeSet.blocked.length ? `              ${count(ctx.changeSet.blocked.length, "row")} waiting on proposals left out; see report.json.` : null,
        ].filter(l => l !== null).map(l => `  ${l}`.replace(/\*\//g, "* /"));
        return ["/" + "*".repeat(112), ...lines, "*".repeat(113) + "/"].join("\n");
    }

    _issueBody(ctx, plan, name, exportId) {
        const { meta, report } = ctx;
        const byTable = list => Object.entries(list.reduce((m, x) => ((m[x.table] = (m[x.table] || 0) + 1), m), {}))
            .map(([t, n]) => `- \`${t}\`: ${n}`).join("\n") || "- none";
        return [
            `SDF import for site(s) ${meta.get("site_ids")} (${String(meta.get("site_names") || "").split("\n").join(", ")}).`,
            "",
            `Generated from export \`${exportId}\`, exported ${meta.get("exported_at")} by ${meta.get("exported_by") || "anonymous"} from \`${meta.get("database_name")}\`.`,
            "",
            "### Inserts", byTable(plan.inserts), "",
            "### Updates", byTable(plan.updates), "",
            "### Deletes", byTable(plan.deletes), "",
            ctx.changeSet.conflicts.length ? `${ctx.changeSet.conflicts.length} conflicting row(s) were left out.` : "",
            ctx.changeSet.proposals.length ? `${ctx.changeSet.proposals.length} proposal(s) are filed separately; see \`proposals/\`.` : "",
            ctx.changeSet.blocked.length ? `${ctx.changeSet.blocked.length} row(s) depending on proposals were left out; see \`report.json\`.` : "",
            report.database_changes_since_export.length ? `${report.database_changes_since_export.length} change request(s) were deployed between export and generation.` : "",
        ].filter((l, i, a) => l !== "" || a[i - 1] !== "").join("\n");
    }

    /** §10: one draft issue per proposal, for sead_change_control. */
    _proposalIssues(ctx, name, exportId) {
        const out = [];
        ctx.changeSet.proposals.forEach((p, n) => {
            const num = String(n + 1).padStart(2, "0");
            let title, label, body;
            if (p.kind === "schema" && p.type === "column") {
                title = `Proposed column ${p.table}.${p.column} (SDF)`;
                label = "schema-change";
                body = [
                    `A curator added a column \`${p.column}\` to the \`${p.sheet}\` sheet of an SDF workbook.`, "",
                    `- Inferred type: ${p.inferred_type}`,
                    `- Rows with a value: ${p.rows_with_values}`,
                    `- Sample values: ${p.sample_values.map(v => `\`${v}\``).join(", ")}`, "",
                    "Suggested starting point (not a decision: types, constraints and naming need design):", "",
                    "```sql", `alter table public.${p.table} add column ${p.column} ${sqlType(p.inferred_type)};`, "```",
                ];
            }
            else if (p.kind === "schema" && p.type === "table") {
                title = `Proposed table ${p.proposed_table} (SDF)`;
                label = "schema-change";
                body = [
                    `A curator added a sheet \`${p.sheet}\` to an SDF workbook, proposing a new table.`, "",
                    `- Primary key: \`${p.primary_key}\``,
                    `- Attaches to: ${p.attaches_to.map(a => `\`${a.table}\` via \`${a.column}\``).join(", ")}`,
                    `- Rows: ${p.row_count}`, "",
                    "| Column | Inferred type | Samples |", "|---|---|---|",
                    ...p.columns.map(c => `| \`${c.name}\` | ${c.inferred_type} | ${c.sample_values.map(v => `\`${v}\``).join(", ")} |`),
                    "", "Suggested starting point (not a decision):", "", "```sql",
                    `create table public.${p.proposed_table} (`,
                    ...p.columns.map((c, i) => `    ${c.name} ${c.name === p.primary_key ? "serial primary key" :
                        (p.attaches_to.find(a => a.column === c.name) ? `integer not null references public.${p.attaches_to.find(a => a.column === c.name).table}` : sqlType(c.inferred_type))}${i < p.columns.length - 1 ? "," : ""}`),
                    ");", "```",
                ];
            }
            else {
                title = `Proposed ${p.op} in ${p.table} (SDF)`;
                label = "new-data";
                body = [
                    `A curator proposed to ${p.op} a row of the shared list \`${p.table}\` (sheet \`${p.sheet}\`, row ${p.row}).`, "",
                    p.op === "update"
                        ? ["| Column | Now | Proposed |", "|---|---|---|", ...p.fields.map(f => `| \`${f.column}\` | ${fmt(f.before)} | ${fmt(f.after)} |`)].join("\n")
                        : p.op === "insert"
                            ? ["| Column | Value |", "|---|---|", ...Object.entries(p.values).map(([k, v]) => `| \`${k}\` | ${fmt(v)} |`)].join("\n")
                            : `Row id: ${p.id}`,
                ];
            }
            const text = [`# ${title}`, "", `Labels: ${label}`, "", ...body, "",
                `Source: SDF export \`${exportId}\`, change request \`${name}\`. Changes that depend on this proposal were left out of that change request and are listed in its report.json; once this is released, the curator makes them again in a fresh export.`, ""].join("\n");
            out.push([`${num}-${slug(title)}.md`, text]);
        });
        return out;
    }
}

/**
 * A SQL string literal. Text with line breaks or other control characters uses
 * an escape string, so the script stays one statement per line and no line
 * ending inside a value can be altered by git or an editor.
 */
function stringLiteral(text) {
    if (!/[\x00-\x1f\x7f\\]/.test(text)) {
        return `'${text.replace(/'/g, "''")}'`;
    }
    const escaped = text.replace(/[\x00-\x1f\x7f\\']/g, ch => {
        switch (ch) {
            case "\\": return "\\\\";
            case "'": return "\\'";
            case "\n": return "\\n";
            case "\r": return "\\r";
            case "\t": return "\\t";
            default: return `\\x${ch.charCodeAt(0).toString(16).padStart(2, "0")}`;
        }
    });
    return `E'${escaped}'`;
}

function insertKey(e) {
    return e.token || `#${e.sheet}!${e.row}`;
}

function count(n, noun) {
    return `${n} ${noun}${n === 1 ? "" : "s"}`;
}

function fmt(v) {
    return v === null || v === undefined ? "*(empty)*" : `\`${String(v).replace(/\|/g, "\\|").replace(/\n/g, " ")}\``;
}

function slug(text) {
    return text.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60);
}

function sqlType(inferred) {
    if (/^integer/.test(inferred)) return "integer";
    if (inferred === "numeric") return "numeric";
    if (inferred === "boolean") return "boolean";
    if (inferred === "date") return "date";
    const m = /longest value (\d+)/.exec(inferred);
    if (m) return `character varying(${Math.max(50, Math.ceil(Number(m[1]) * 2 / 50) * 50)})`;
    return "text";
}
