import { SdfError, quoteIdent } from "./SdfCommon.js";

/**
 * The schema model behind an SDF export (spec §3, §5, §6).
 *
 * Everything the exporter knows about tables and columns comes from here, and
 * everything here is computed from pg_catalog at export time rather than from a
 * hard-coded list. A table or column added through change control therefore
 * appears in the next export without a code change, which is what §3 requires.
 *
 * The only hand-maintained knowledge is the small set of decisions the
 * specification makes by name: which tables are added to the ownership closure
 * explicitly, which referenced tables are shipped used-only, which tables are
 * deprecated, and the label expressions that a plain name column cannot supply.
 */

const ROOT_TABLE = "tbl_sites";

//§3: owned tables the closure cannot find because they reach a site against
//the direction of the walk. Each is reached through a child that is owned.
const EXPLICIT_OWNED = [
    //datasets reach a site through their analysis entities
    { table: "tbl_datasets", via: { child: "tbl_analysis_entities", column: "dataset_id" } },
    //a lookup by structure, but every feature in use belongs to exactly one site
    { table: "tbl_features", via: { child: "tbl_physical_sample_features", column: "feature_id" } },
];

//§6: referenced tables shipped with only the rows this bundle uses. Every other
//referenced table is shipped in full.
const USED_ONLY_REFERENCES = new Set([
    "tbl_taxa_tree_master",
    "tbl_biblio",
    "tbl_locations",
    "tbl_relative_ages",
    "tbl_projects",
    "tbl_contacts",
]);

//§3: deprecated legacy tables. "excluded" when the modern store holds all of
//their information; "read-only" while they still hold something it lacks.
const DEPRECATED = {
    tbl_dendro: "excluded",
    tbl_dendro_date_notes: "excluded",
    tbl_dendro_dates: "read-only", //OQ-24, sead_change_control#451
};

//§5: columns the database maintains itself. Exported, never read back.
const SYSTEM_COLUMNS = new Set(["date_updated"]);

//§5: label expressions over alias `t` where the table's name column is not
//enough. Anything not listed falls back to the rule in labelExpression().
const LABEL_EXPRESSIONS = {
    tbl_analysis_entities: `concat_ws(' · ',
        (select x.sample_name from public.tbl_physical_samples x where x.physical_sample_id = t.physical_sample_id),
        (select x.dataset_name from public.tbl_datasets x where x.dataset_id = t.dataset_id))`,
    tbl_taxa_tree_master: `concat_ws(' ',
        (select x.genus_name from public.tbl_taxa_tree_genera x where x.genus_id = t.genus_id),
        t.species,
        (select x.author_name from public.tbl_taxa_tree_authors x where x.author_id = t.author_id))`,
    tbl_biblio: `coalesce(nullif(t.bugs_reference, ''), nullif(concat_ws(', ', left(t.authors, 60), t.year), ''), left(t.title, 80))`,
    tbl_contacts: `nullif(concat_ws(' ', t.first_name, t.last_name), '')`,
    tbl_coordinate_method_dimensions: `concat_ws(' · ',
        (select x.dimension_name from public.tbl_dimensions x where x.dimension_id = t.dimension_id),
        (select x.method_name from public.tbl_methods x where x.method_id = t.method_id))`,
};

//Columns never chosen as a label by the fallback rule: they describe a row
//rather than name it.
const NON_LABEL_COLUMN = /(^|_)(description|notes?|comments?|abbrev|abbreviation|url|uuid|email)$/;

export default class SdfSchema {

    static async load(client) {
        const schema = new SdfSchema();
        await schema._introspect(client);
        schema._buildOwnership();
        schema._buildReferences();
        schema._assignLabels();
        return schema;
    }

    async _introspect(client) {
        const tables = await client.query(`
            select c.relname as name, obj_description(c.oid, 'pg_class') as comment
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = 'public' and c.relkind in ('r', 'p')`);

        const columns = await client.query(`
            select c.relname as table_name, a.attname as name, a.attnum as position,
                   format_type(a.atttypid, a.atttypmod) as pg_type, format_type(a.atttypid, null) as base_type, t.typname,
                   a.attnotnull as not_null, a.attgenerated <> '' as generated, a.atthasdef as has_default,
                   case when t.typname in ('varchar', 'bpchar') and a.atttypmod > 0 then a.atttypmod - 4 end as max_length,
                   case when t.typname = 'numeric' and a.atttypmod > 0 then ((a.atttypmod - 4) >> 16) & 65535 end as numeric_precision,
                   case when t.typname = 'numeric' and a.atttypmod > 0 then (a.atttypmod - 4) & 65535 end as numeric_scale,
                   col_description(a.attrelid, a.attnum) as comment
            from pg_attribute a
            join pg_class c on c.oid = a.attrelid
            join pg_namespace n on n.oid = c.relnamespace
            join pg_type t on t.oid = a.atttypid
            where n.nspname = 'public' and c.relkind in ('r', 'p')
              and a.attnum > 0 and not a.attisdropped
            order by c.relname, a.attnum`);

        const constraints = await client.query(`
            select con.conname as name, con.contype as type,
                   c.relname as table_name, pc.relname as parent_table,
                   array(select a.attname from unnest(con.conkey) with ordinality k(attnum, ord)
                         join pg_attribute a on a.attrelid = con.conrelid and a.attnum = k.attnum
                         order by k.ord)::text[] as columns,
                   array(select a.attname from unnest(con.confkey) with ordinality k(attnum, ord)
                         join pg_attribute a on a.attrelid = con.confrelid and a.attnum = k.attnum
                         order by k.ord)::text[] as parent_columns
            from pg_constraint con
            join pg_class c on c.oid = con.conrelid
            join pg_namespace n on n.oid = c.relnamespace
            left join pg_class pc on pc.oid = con.confrelid
            left join pg_namespace pn on pn.oid = pc.relnamespace
            where n.nspname = 'public' and con.contype in ('p', 'f')
              and (con.contype = 'p' or pn.nspname = 'public')
            order by con.conname`);

        //unique constraints and unique indexes, excluding partial and expression
        //indexes, which the validator cannot evaluate (the database still does)
        const uniques = await client.query(`
            select c.relname as table_name, i.relname as name,
                   array(select a.attname from unnest(x.indkey) with ordinality k(attnum, ord)
                         join pg_attribute a on a.attrelid = x.indrelid and a.attnum = k.attnum
                         order by k.ord)::text[] as columns
            from pg_index x
            join pg_class c on c.oid = x.indrelid
            join pg_class i on i.oid = x.indexrelid
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = 'public' and x.indisunique and not x.indisprimary
              and x.indpred is null and x.indexprs is null`);

        this.tables = new Map();
        for (const row of tables.rows) {
            this.tables.set(row.name, {
                name: row.name,
                sheet: row.name.replace(/^tbl_/, ""),
                comment: row.comment,
                columns: [],
                pk: null,
                pkColumns: [],
                fks: [],       //outgoing
                incoming: [],  //fks pointing at this table
                uniques: [],   //[{ name, columns }]
            });
        }
        for (const row of columns.rows) {
            const table = this.tables.get(row.table_name);
            if (!table) continue;
            table.columns.push({
                name: row.name,
                position: row.position,
                pgType: row.pg_type,
                //unbounded form for casts; "character" alone would mean char(1)
                baseType: row.typname === "bpchar" ? "text" : row.base_type,
                typname: row.typname,
                nullable: !row.not_null,
                generated: row.generated,
                hasDefault: row.has_default,
                maxLength: row.max_length,
                numericPrecision: row.numeric_precision,
                numericScale: row.numeric_scale,
                comment: row.comment,
                system: SYSTEM_COLUMNS.has(row.name),
                valueKind: valueKindOf(row.typname),
            });
        }
        for (const row of constraints.rows) {
            const table = this.tables.get(row.table_name);
            if (!table) continue;
            if (row.type === "p") {
                table.pkColumns = row.columns;
                table.pk = row.columns.length === 1 ? row.columns[0] : null;
                continue;
            }
            const fk = {
                name: row.name,
                table: row.table_name,
                columns: row.columns,
                column: row.columns.length === 1 ? row.columns[0] : null,
                parent: row.parent_table,
                parentColumns: row.parent_columns,
                parentColumn: row.parent_columns.length === 1 ? row.parent_columns[0] : null,
            };
            table.fks.push(fk);
            const parent = this.tables.get(row.parent_table);
            if (parent) parent.incoming.push(fk);
        }
        for (const table of this.tables.values()) {
            for (const column of table.columns) {
                column.fk = table.fks.find(fk => fk.column === column.name) || null;
            }
        }
        const seen = new Set();
        for (const row of uniques.rows) {
            const table = this.tables.get(row.table_name);
            const key = `${row.table_name}:${row.columns.join(",")}`;
            //several tables carry the same constraint twice under different names
            if (!table || seen.has(key)) continue;
            seen.add(key);
            table.uniques.push({ name: row.name, columns: row.columns });
        }
    }

    /**
     * §3: the owned tables are found by walking the foreign-key graph outward from
     * tbl_sites, following only edges that point into an already-included table,
     * then adding the explicit tables and whatever hangs off them. Each owned
     * table records the edges that make it owned - those, and only those, decide
     * which of its rows belong to a site.
     */
    _buildOwnership() {
        this.owned = new Map(); //name -> { table, level, edges, reach, via }

        const root = this._table(ROOT_TABLE);
        this.owned.set(root.name, { table: root, level: 0, edges: [], reach: "root" });

        let frontier = [root.name];
        let level = 0;
        while (frontier.length) {
            level++;
            const next = new Set();
            for (const parentName of frontier) {
                for (const fk of this._table(parentName).incoming) {
                    if (fk.table !== fk.parent && !this.owned.has(fk.table)) {
                        next.add(fk.table);
                    }
                }
            }
            for (const name of [...next].sort()) {
                this.owned.set(name, { table: this._table(name), level, edges: [], reach: "closure" });
            }
            frontier = [...next];
        }

        //an ownership edge is any fk from a closure table to another closure table
        for (const entry of this.owned.values()) {
            if (entry.reach !== "closure") continue;
            entry.edges = entry.table.fks.filter(fk => fk.parent !== fk.table && this.owned.has(fk.parent));
        }

        //explicit additions, and the tables that hang off them (dataset contacts,
        //methods and submissions). Their only ownership edge is to the explicit table.
        const closureNames = new Set(this.owned.keys());
        for (const spec of EXPLICIT_OWNED) {
            const table = this._table(spec.table);
            const viaTable = this._table(spec.via.child);
            const viaFk = viaTable.fks.find(fk => fk.column === spec.via.column && fk.parent === table.name);
            if (!viaFk) {
                throw new SdfError("unsupported_schema", `${spec.via.child}.${spec.via.column} no longer references ${spec.table}.`);
            }
            this.owned.set(table.name, {
                table, level: this.owned.get(viaTable.name).level + 1, edges: [], reach: "reverse", via: viaFk,
            });
            for (const fk of table.incoming) {
                if (fk.table === table.name || closureNames.has(fk.table) || this.owned.has(fk.table)) continue;
                this.owned.set(fk.table, {
                    table: this._table(fk.table),
                    level: this.owned.get(table.name).level + 1,
                    edges: [fk],
                    reach: "explicit-child",
                });
            }
        }

        for (const entry of this.owned.values()) {
            entry.deprecated = DEPRECATED[entry.table.name] || null;
            this._requireSimpleKeys(entry.table);
        }

        //§4 sheet order: ownership level, then name, except that the dataset tables
        //follow tbl_analysis_entities and tbl_features follows tbl_physical_samples.
        const byLevel = [...this.owned.values()]
            .filter(e => e.reach === "root" || e.reach === "closure")
            .sort((a, b) => a.level - b.level || a.table.name.localeCompare(b.table.name))
            .map(e => e.table.name);
        const insertAfter = (list, anchor, names) => {
            const at = list.indexOf(anchor);
            list.splice(at < 0 ? list.length : at + 1, 0, ...names);
        };
        const datasetGroup = ["tbl_datasets", ...[...this.owned.values()]
            .filter(e => e.reach === "explicit-child" && e.edges[0].parent === "tbl_datasets")
            .map(e => e.table.name).sort()];
        const featureGroup = ["tbl_features", ...[...this.owned.values()]
            .filter(e => e.reach === "explicit-child" && e.edges[0].parent === "tbl_features")
            .map(e => e.table.name).sort()];
        insertAfter(byLevel, "tbl_analysis_entities", datasetGroup);
        insertAfter(byLevel, "tbl_physical_samples", featureGroup);
        this.ownedOrder = byLevel;

        //Fetch order: a table is fetched after everything its selection depends on.
        //Closure tables by level; then datasets and their children; then features.
        this.fetchOrder = [
            ...[...this.owned.values()]
                .filter(e => e.reach === "root" || e.reach === "closure")
                .sort((a, b) => a.level - b.level || a.table.name.localeCompare(b.table.name))
                .map(e => e.table.name),
            ...datasetGroup,
            ...featureGroup,
        ];
    }

    /**
     * §3 / §6: referenced tables are the targets of foreign keys from exported
     * owned tables that are not owned themselves.
     */
    _buildReferences() {
        this.referenced = new Map(); //name -> { table, mode }
        for (const entry of this.owned.values()) {
            if (entry.deprecated === "excluded") continue;
            for (const fk of entry.table.fks) {
                if (this.owned.has(fk.parent) || this.referenced.has(fk.parent)) continue;
                const table = this._table(fk.parent);
                this._requireSimpleKeys(table);
                this.referenced.set(table.name, {
                    table,
                    mode: USED_ONLY_REFERENCES.has(table.name) ? "used-only" : "full",
                });
            }
        }
        this.referencedOrder = [...this.referenced.keys()].sort((a, b) =>
            this._table(a).sheet.localeCompare(this._table(b).sheet));
    }

    _assignLabels() {
        for (const table of this.tables.values()) {
            table.labelExpression = labelExpression(table);
        }
    }

    _requireSimpleKeys(table) {
        if (!table.pk) {
            throw new SdfError("unsupported_schema",
                `${table.name} has ${table.pkColumns.length ? "a composite" : "no"} primary key; SDF needs a single-column key (§7).`);
        }
        for (const fk of table.fks) {
            if (!fk.column) {
                throw new SdfError("unsupported_schema",
                    `${table.name} has a composite foreign key (${fk.name}); SDF binds single-column keys only (§7).`);
            }
        }
    }

    _table(name) {
        const table = this.tables.get(name);
        if (!table) {
            throw new SdfError("unsupported_schema", `Table ${name} does not exist in the public schema.`);
        }
        return table;
    }

    table(name) {
        return this._table(name);
    }

    /** The exported (non-generated) columns of a table, in ordinal order. */
    exportedColumns(table) {
        return table.columns.filter(c => !c.generated);
    }

    column(table, name) {
        return table.columns.find(c => c.name === name);
    }

    /** "owned" | "reference" | null, and the deprecation state, of a table. */
    roleOf(tableName) {
        if (this.owned.has(tableName)) return "owned";
        if (this.referenced.has(tableName)) return "reference";
        return null;
    }

    deprecationOf(tableName) {
        const entry = this.owned.get(tableName);
        return entry ? entry.deprecated : null;
    }

    /** The table a sheet name denotes (§4: table name without tbl_), if any. */
    tableForSheet(sheetName) {
        return this.tables.get(`tbl_${sheetName}`) || null;
    }

    /**
     * §3: the foreign keys of an owned table that tie its rows to a site - its
     * ownership edges, plus the keys through which a reverse-reached table
     * (datasets, features) belongs to a site at all.
     */
    ownershipKeys(tableName) {
        const entry = this.owned.get(tableName);
        if (!entry) return [];
        const keys = [...entry.edges];
        for (const other of this.owned.values()) {
            if (other.reach === "reverse" && other.via.table === tableName) keys.push(other.via);
        }
        return keys;
    }

    /** The reverse-reached tables (datasets, features) and the key that reaches each. */
    reverseReached() {
        return [...this.owned.values()].filter(e => e.reach === "reverse").map(e => ({ table: e.table.name, via: e.via }));
    }

    /** Primary-key column names of the owned tables, for new-sheet attachment (§10). */
    ownedKeyColumns() {
        const keys = new Map();
        for (const entry of this.owned.values()) {
            if (!entry.deprecated) keys.set(entry.table.pk, entry.table.name);
        }
        return keys;
    }
}

/**
 * How a PostgreSQL type is carried in a cell (§8): integers and decimals as
 * numbers, booleans as booleans, everything else as text.
 */
function valueKindOf(typname) {
    switch (typname) {
        case "int2": case "int4": case "int8":
            return "integer";
        case "numeric": case "float4": case "float8":
            return "decimal";
        case "bool":
            return "boolean";
        default:
            return "text";
    }
}

/**
 * §5: the label expression for a table, over alias `t`. Explicit where listed;
 * otherwise the table's name column (`name`, or the first `*_name`); otherwise
 * its first plain text column that is not a description; otherwise the primary
 * key, prefixed with the sheet name.
 */
function labelExpression(table) {
    if (LABEL_EXPRESSIONS[table.name]) {
        return LABEL_EXPRESSIONS[table.name].replace(/\s+/g, " ");
    }
    const textual = table.columns.filter(c =>
        !c.generated && ["text", "varchar", "bpchar"].includes(c.typname));
    const named = textual.find(c => c.name === "name")
        || textual.find(c => /_name$/.test(c.name) && !/abbrev|_alt_/.test(c.name));
    if (named) {
        return `t.${quoteIdent(named.name)}`;
    }
    const plain = textual.find(c => !NON_LABEL_COLUMN.test(c.name));
    if (plain) {
        return `t.${quoteIdent(plain.name)}`;
    }
    const pk = table.pk || (table.columns[0] && table.columns[0].name);
    return `'${table.sheet.replace(/'/g, "''")} ' || t.${quoteIdent(pk)}::text`;
}
