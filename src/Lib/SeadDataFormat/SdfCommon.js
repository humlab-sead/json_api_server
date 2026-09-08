/**
 * SEAD Data Format (SDF) — shared constants and helpers.
 *
 * SDF is a site-scoped, flattened representation of everything a site owns,
 * described in plans/sead-data-format-design.html. This module is the single
 * place the exporter and the importer agree on: the format version, the
 * round-trip vocabulary, the column-ordering rule, and a small SheetBuilder
 * that both the tabular output and its manifest are derived from.
 *
 * The export endpoint returns this structure as JSON — sheets of columns and
 * rows plus a manifest — rather than a rendered .xlsx. Turning a sheet into a
 * worksheet is a client-side concern (one header row, rows already aligned to
 * the column order); the server owns the format, not the file.
 */

//D9: the format's own version. Bump the minor for added sheets/columns, the
//major for a change in meaning of something that already exists.
export const SDF_VERSION = "SDF/1.0";

//D5: every column declares whether it round-trips.
export const ROUNDTRIP = {
    EDITABLE: "editable",   //written back on import
    REFERENCE: "reference", //real data, not editable through this sheet
    DERIVED: "derived",     //computed for convenience, ignored on import
};

//Column-ordering rule (see "Column order, decided" in the design). finalize()
//sorts stably by this group, then by insertion order within the group.
export const COL_GROUP = {
    IDENTITY: 0, //primary key, parent FKs, uuid — hidden + locked
    ACTION: 1,   //D12 delete marker — first visible column
    CONTEXT: 2,  //parent name, keeps a row self-describing when sorted/filtered
    OWN: 3,      //the entity's own fields
    PIVOT: 4,    //absorbed satellites, prefixed with their source
    DERIVED: 5,  //derived conveniences
    UPDATED: 6,  //date_updated, always last
};

//D12: the delete marker. Blank means "leave this record alone apart from field
//edits in the row"; "delete" is a positive act, visible in the sheet.
export const ACTION_VALUES = ["", "delete"];

export const NEW_TOKEN_RE = /^NEW(-[A-Za-z0-9_]+)+$/;

export function isBlank(v) {
    return v === null || v === undefined || v === "" || v === "None";
}

export function isNewToken(v) {
    return typeof v === "string" && NEW_TOKEN_RE.test(v.trim());
}

/** Normalises the "None"/null/"" family to null, trims strings, leaves the rest. */
export function clean(v) {
    if (isBlank(v)) return null;
    if (typeof v === "string") {
        const t = v.trim();
        return t === "" ? null : t;
    }
    return v;
}

/** sha256 over a stable JSON serialisation, for the manifest checksum. */
export function checksum(obj, crypto) {
    const json = JSON.stringify(obj);
    return "sha256:" + crypto.createHash("sha256").update(json, "utf8").digest("hex");
}

/**
 * Accumulates the columns and rows of one sheet.
 *
 * Columns are declared lazily — the first mention of a key wins, which is what
 * lets pivot columns be created on demand as rows are walked. Rows are held as
 * plain objects keyed by column key and only flattened to arrays in finalize(),
 * so column order is decided once, after every row has been seen.
 */
export class SheetBuilder {
    constructor(name, tier, grain, { note = null } = {}) {
        this.name = name;
        this.tier = tier;      //"A" entities, "B" observations, "C" references, "D" machine
        this.grain = grain;    //human description of "one row per ..."
        this.note = note;
        this.sourceTables = new Set();
        this._cols = new Map(); //key -> column descriptor
        this._rows = [];        //array of plain objects
        this._seq = 0;
    }

    /**
     * Declare (or fetch) a column.
     *   key        stable identifier, also the row-object key
     *   title      human header (row 1 in Excel); defaults to key
     *   source     manifest binding, e.g. "public.tbl_sites.site_name"
     *              or "pivot:tbl_sample_descriptions:Wood function"
     *   roundtrip  ROUNDTRIP.*
     *   group      COL_GROUP.*
     *   hidden/locked  Excel affordances (identity block, derived, references)
     *   type       "text" | "number" | "integer" | "enum" | "date"
     *   vocab      name of the Vocabularies list backing this column, if any
     *   pivotType  the type/class value this pivot column was generated from
     */
    col(key, opts = {}) {
        if (!this._cols.has(key)) {
            this._cols.set(key, {
                key,
                title: opts.title || key,
                source: opts.source || null,
                roundtrip: opts.roundtrip || ROUNDTRIP.EDITABLE,
                group: opts.group === undefined ? COL_GROUP.OWN : opts.group,
                hidden: !!opts.hidden,
                locked: !!opts.locked,
                type: opts.type || "text",
                vocab: opts.vocab || null,
                pivotType: opts.pivotType === undefined ? null : opts.pivotType,
                seq: this._seq++,
            });
            if (opts.table) this.sourceTables.add(opts.table);
        }
        return key;
    }

    /** Identity-block column: hidden, locked, reference, ordered first. */
    idCol(key, opts = {}) {
        return this.col(key, {
            ...opts,
            roundtrip: ROUNDTRIP.REFERENCE,
            group: COL_GROUP.IDENTITY,
            hidden: true,
            locked: true,
        });
    }

    /** The D12 Action column. Call once, early, on every editable sheet. */
    actionCol() {
        return this.col("Action", {
            title: "Action",
            source: null,
            roundtrip: ROUNDTRIP.EDITABLE,
            group: COL_GROUP.ACTION,
            type: "enum",
            vocab: "action",
        });
    }

    addRow(obj) {
        this._rows.push(obj);
        return obj;
    }

    get rowCount() {
        return this._rows.length;
    }

    /** Ordered column descriptors, after the ordering rule is applied. */
    orderedColumns() {
        return [...this._cols.values()].sort((a, b) =>
            a.group - b.group || a.seq - b.seq);
    }

    /** { name, tier, grain, note, columns, rows } — rows aligned to columns. */
    finalize() {
        const cols = this.orderedColumns();
        const rows = this._rows.map(r => cols.map(c => {
            const v = r[c.key];
            return v === undefined ? null : v;
        }));
        return {
            name: this.name,
            tier: this.tier,
            grain: this.grain,
            note: this.note,
            source_tables: [...this.sourceTables].sort(),
            columns: cols.map(({ seq, ...rest }) => rest),
            rows,
        };
    }
}
